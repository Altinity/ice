/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.altinity.ice.rest.catalog.internal.rest;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

public class RESTCatalogAdapter implements RESTCatalogHandler {

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;

  public RESTCatalogAdapter(Catalog catalog) {
    this.catalog = catalog;
    this.asNamespaceCatalog =
        catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
    this.asViewCatalog = catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
  }

  @Override
  public <T extends RESTResponse> T handle(
      Session session,
      Route route,
      Map<String, String> vars,
      Object requestBody,
      Class<T> responseType) {
    switch (route) {
      case TOKENS:
        return castResponse(responseType, handleOAuthRequest(requestBody));

      case CONFIG:
        return castResponse(
            responseType,
            ConfigResponse.builder()
                .withEndpoints(
                    Arrays.stream(Route.values())
                        .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                        .collect(Collectors.toList()))
                .build());

      case LIST_NAMESPACES:
        if (asNamespaceCatalog != null) {
          Namespace ns;
          if (vars.containsKey("parent")) {
            ns =
                Namespace.of(
                    RESTUtil.NAMESPACE_SPLITTER
                        .splitToStream(vars.get("parent"))
                        .toArray(String[]::new));
          } else {
            ns = Namespace.empty();
          }

          String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
          String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);

          if (pageSize != null) {
            return castResponse(
                responseType,
                CatalogHandlers.listNamespaces(asNamespaceCatalog, ns, pageToken, pageSize));
          } else {
            return castResponse(
                responseType, CatalogHandlers.listNamespaces(asNamespaceCatalog, ns));
          }
        }
        break;

      case CREATE_NAMESPACE:
        if (asNamespaceCatalog != null) {
          CreateNamespaceRequest request = castRequest(CreateNamespaceRequest.class, requestBody);
          return castResponse(
              responseType, CatalogHandlers.createNamespace(asNamespaceCatalog, request));
        }
        break;

      case NAMESPACE_EXISTS:
        if (asNamespaceCatalog != null) {
          CatalogHandlers.namespaceExists(asNamespaceCatalog, namespaceFromPathVars(vars));
          return null;
        }
        break;

      case LOAD_NAMESPACE:
        if (asNamespaceCatalog != null) {
          Namespace namespace = namespaceFromPathVars(vars);
          return castResponse(
              responseType, CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace));
        }
        break;

      case DROP_NAMESPACE:
        if (asNamespaceCatalog != null) {
          CatalogHandlers.dropNamespace(asNamespaceCatalog, namespaceFromPathVars(vars));
          return null;
        }
        break;

      case UPDATE_NAMESPACE:
        if (asNamespaceCatalog != null) {
          Namespace namespace = namespaceFromPathVars(vars);
          UpdateNamespacePropertiesRequest request =
              castRequest(UpdateNamespacePropertiesRequest.class, requestBody);
          return castResponse(
              responseType,
              CatalogHandlers.updateNamespaceProperties(asNamespaceCatalog, namespace, request));
        }
        break;

      case LIST_TABLES:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
          String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
          if (pageSize != null) {
            return castResponse(
                responseType, CatalogHandlers.listTables(catalog, namespace, pageToken, pageSize));
          } else {
            return castResponse(responseType, CatalogHandlers.listTables(catalog, namespace));
          }
        }

      case CREATE_TABLE:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          CreateTableRequest request = castRequest(CreateTableRequest.class, requestBody);
          request.validate();
          if (request.stageCreate()) {
            return castResponse(
                responseType, CatalogHandlers.stageTableCreate(catalog, namespace, request));
          } else {
            return castResponse(
                responseType, CatalogHandlers.createTable(catalog, namespace, request));
          }
        }

      case DROP_TABLE:
        {
          if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
            CatalogHandlers.purgeTable(catalog, tableIdentFromPathVars(vars));
          } else {
            CatalogHandlers.dropTable(catalog, tableIdentFromPathVars(vars));
          }
          return null;
        }

      case TABLE_EXISTS:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          CatalogHandlers.tableExists(catalog, ident);
          return null;
        }

      case LOAD_TABLE:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          return castResponse(responseType, CatalogHandlers.loadTable(catalog, ident));
        }

      case REGISTER_TABLE:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          RegisterTableRequest request = castRequest(RegisterTableRequest.class, requestBody);
          return castResponse(
              responseType, CatalogHandlers.registerTable(catalog, namespace, request));
        }

      case UPDATE_TABLE:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          UpdateTableRequest request = castRequest(UpdateTableRequest.class, requestBody);
          return castResponse(responseType, CatalogHandlers.updateTable(catalog, ident, request));
        }

      case RENAME_TABLE:
        {
          RenameTableRequest request = castRequest(RenameTableRequest.class, requestBody);
          CatalogHandlers.renameTable(catalog, request);
          return null;
        }

      case REPORT_METRICS:
        // nothing to do here other than checking that we're getting the correct request
        castRequest(ReportMetricsRequest.class, requestBody);
        return null;

      case COMMIT_TRANSACTION:
        {
          CommitTransactionRequest request =
              castRequest(CommitTransactionRequest.class, requestBody);
          commitTransaction(catalog, request);
          return null;
        }

      case LIST_VIEWS:
        {
          if (null != asViewCatalog) {
            Namespace namespace = namespaceFromPathVars(vars);
            String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
            String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
            if (pageSize != null) {
              return castResponse(
                  responseType,
                  CatalogHandlers.listViews(asViewCatalog, namespace, pageToken, pageSize));
            } else {
              return castResponse(
                  responseType, CatalogHandlers.listViews(asViewCatalog, namespace));
            }
          }
          break;
        }

      case CREATE_VIEW:
        {
          if (null != asViewCatalog) {
            Namespace namespace = namespaceFromPathVars(vars);
            CreateViewRequest request = castRequest(CreateViewRequest.class, requestBody);
            return castResponse(
                responseType, CatalogHandlers.createView(asViewCatalog, namespace, request));
          }
          break;
        }

      case VIEW_EXISTS:
        {
          if (null != asViewCatalog) {
            CatalogHandlers.viewExists(asViewCatalog, viewIdentFromPathVars(vars));
            return null;
          }
          break;
        }

      case LOAD_VIEW:
        {
          if (null != asViewCatalog) {
            TableIdentifier ident = viewIdentFromPathVars(vars);
            return castResponse(responseType, CatalogHandlers.loadView(asViewCatalog, ident));
          }
          break;
        }

      case UPDATE_VIEW:
        {
          if (null != asViewCatalog) {
            TableIdentifier ident = viewIdentFromPathVars(vars);
            UpdateTableRequest request = castRequest(UpdateTableRequest.class, requestBody);
            return castResponse(
                responseType, CatalogHandlers.updateView(asViewCatalog, ident, request));
          }
          break;
        }

      case RENAME_VIEW:
        {
          if (null != asViewCatalog) {
            RenameTableRequest request = castRequest(RenameTableRequest.class, requestBody);
            CatalogHandlers.renameView(asViewCatalog, request);
            return null;
          }
          break;
        }

      case DROP_VIEW:
        if (null != asViewCatalog) {
          CatalogHandlers.dropView(asViewCatalog, viewIdentFromPathVars(vars));
          return null;
        }
        break;
    }

    return null;
  }

  private static OAuthTokenResponse handleOAuthRequest(Object body) {
    Map<String, String> request = (Map<String, String>) castRequest(Map.class, body);
    String grantType = request.get("grant_type");
    switch (grantType) {
      case "client_credentials":
        return OAuthTokenResponse.builder()
            .withToken("client-credentials-token:sub=" + request.get("client_id"))
            .withTokenType("Bearer")
            .build();

      case "urn:ietf:params:oauth:grant-type:token-exchange":
        String actor = request.get("actor_token");
        String token =
            String.format(
                "token-exchange-token:sub=%s%s",
                request.get("subject_token"), actor != null ? ",act=" + actor : "");
        return OAuthTokenResponse.builder()
            .withToken(token)
            .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
            .withTokenType("Bearer")
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
    }
  }

  /**
   * This is a very simplistic approach that only validates the requirements for each table and does
   * not do any other conflict detection. Therefore, it does not guarantee true transactional
   * atomicity, which is left to the implementation details of a REST server.
   */
  private static void commitTransaction(Catalog catalog, CommitTransactionRequest request) {
    List<Transaction> transactions = Lists.newArrayList();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      Table table = catalog.loadTable(tableChange.identifier());
      if (table instanceof BaseTable) {
        Transaction transaction =
            Transactions.newTransaction(
                tableChange.identifier().toString(), ((BaseTable) table).operations());
        transactions.add(transaction);

        BaseTransaction.TransactionTable txTable =
            (BaseTransaction.TransactionTable) transaction.table();

        // this performs validations and makes temporary commits that are in-memory
        commit(txTable.operations(), tableChange);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    // only commit if validations passed previously
    transactions.forEach(Transaction::commitTransaction);
  }

  // Copied from CatalogHandlers.commit.
  static TableMetadata commit(TableOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // validate requirements
                try {
                  request.requirements().forEach(requirement -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                // apply changes
                TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(metadataBuilder));

                TableMetadata updated = metadataBuilder.build();
                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }

                // commit
                taskOps.commit(base, updated);
              });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }

  /**
   * Exception used to avoid retrying commits when assertions fail.
   *
   * <p>When a REST assertion fails, it will throw CommitFailedException to send back to the client.
   * But the assertion checks happen in the block that is retried if {@link
   * TableOperations#commit(TableMetadata, TableMetadata)} throws CommitFailedException. This is
   * used to avoid retries for assertion failures, which are unwrapped and rethrown outside of the
   * commit loop.
   */
  private static class ValidationFailureException extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailureException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }

  private static class BadResponseType extends RuntimeException {
    private BadResponseType(Class<?> responseType, Object response) {
      super(
          String.format("Invalid response object, not a %s: %s", responseType.getName(), response));
    }
  }

  private static class BadRequestType extends RuntimeException {
    private BadRequestType(Class<?> requestType, Object request) {
      super(String.format("Invalid request object, not a %s: %s", requestType.getName(), request));
    }
  }

  private static <T> T castRequest(Class<T> requestType, Object request) {
    if (requestType.isInstance(request)) {
      return requestType.cast(request);
    }
    throw new BadRequestType(requestType, request);
  }

  private static <T extends RESTResponse> T castResponse(Class<T> responseType, Object response) {
    if (responseType.isInstance(response)) {
      return responseType.cast(response);
    }
    throw new BadResponseType(responseType, response);
  }

  private static Namespace namespaceFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeNamespace(pathVars.get("namespace"));
  }

  private static TableIdentifier tableIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("table")));
  }

  private static TableIdentifier viewIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("view")));
  }
}

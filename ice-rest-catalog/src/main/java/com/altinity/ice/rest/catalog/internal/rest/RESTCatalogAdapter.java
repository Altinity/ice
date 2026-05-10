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

import com.altinity.ice.rest.catalog.internal.auth.Session;
import com.altinity.ice.rest.catalog.internal.config.CommitRetryConfig;
import com.altinity.ice.rest.catalog.internal.etcd.CommitLock;
import com.altinity.ice.rest.catalog.internal.metrics.CatalogMetrics;
import com.altinity.ice.rest.catalog.internal.metrics.PrometheusMetricsReporter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalogAdapter implements RESTCatalogHandler {

  private static final Logger logger = LoggerFactory.getLogger(RESTCatalogAdapter.class);

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;
  private final CommitRetryConfig commitRetry;
  private final CommitLock commitLock;

  public RESTCatalogAdapter(Catalog catalog) {
    this(catalog, CommitRetryConfig.defaults(), null);
  }

  public RESTCatalogAdapter(Catalog catalog, CommitRetryConfig commitRetry) {
    this(catalog, commitRetry, null);
  }

  public RESTCatalogAdapter(Catalog catalog, CommitRetryConfig commitRetry, CommitLock commitLock) {
    this.catalog = catalog;
    this.asNamespaceCatalog =
        catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
    this.asViewCatalog = catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
    this.commitRetry = commitRetry != null ? commitRetry : CommitRetryConfig.defaults();
    this.commitLock = commitLock;
  }

  private static String namespaceLabel(TableIdentifier ident) {
    String[] levels = ident.namespace().levels();
    return levels.length == 0 ? "" : String.join(".", levels);
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
            if (pageToken == null) {
              pageToken = "";
            }
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
          var response = CatalogHandlers.createNamespace(asNamespaceCatalog, request);
          CatalogMetrics.getInstance().recordNamespaceCreated(catalog.name());
          return castResponse(responseType, response);
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
          CatalogMetrics.getInstance().recordNamespaceDropped(catalog.name());
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
            var response = CatalogHandlers.createTable(catalog, namespace, request);
            CatalogMetrics.getInstance().recordTableCreated(catalog.name());
            return castResponse(responseType, response);
          }
        }

      case DROP_TABLE:
        {
          if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
            CatalogHandlers.purgeTable(catalog, tableIdentFromPathVars(vars));
          } else {
            CatalogHandlers.dropTable(catalog, tableIdentFromPathVars(vars));
          }
          CatalogMetrics.getInstance().recordTableDropped(catalog.name());
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
          var response = updateTable(catalog, ident, request);

          // Check if this update contains schema changes
          boolean hasSchemaUpdate =
              request.updates().stream()
                  .anyMatch(
                      update ->
                          update instanceof MetadataUpdate.AddSchema
                              || update instanceof MetadataUpdate.SetCurrentSchema);
          if (hasSchemaUpdate) {
            PrometheusMetricsReporter.getInstance()
                .recordSchemaUpdate(catalog.name(), ident.namespace().toString(), ident.name());
          }

          return castResponse(responseType, response);
        }

      case RENAME_TABLE:
        {
          RenameTableRequest request = castRequest(RenameTableRequest.class, requestBody);
          CatalogHandlers.renameTable(catalog, request);
          return null;
        }

      case REPORT_METRICS:
        {
          ReportMetricsRequest request = castRequest(ReportMetricsRequest.class, requestBody);
          PrometheusMetricsReporter metricsReporter = PrometheusMetricsReporter.getInstance();
          if (metricsReporter != null && request.report() != null) {
            String catalogName = catalog.name();
            metricsReporter.report(catalogName, request.report());
          }
          return null;
        }

      case COMMIT_TRANSACTION:
        {
          CommitTransactionRequest request =
              castRequest(CommitTransactionRequest.class, requestBody);
          commitTransaction(request);
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

  private LoadTableResponse updateTable(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    if (commitLock != null) {
      try (CommitLock.Handle ignored = commitLock.acquire(ident)) {
        return loadAndCommitTable(catalog, ident, request);
      }
    }
    return loadAndCommitTable(catalog, ident, request);
  }

  /**
   * Matches {@link CatalogHandlers#updateTable(Catalog, TableIdentifier, UpdateTableRequest)} so
   * staged-create commits ({@link UpdateRequirement.AssertTableDoesNotExist}) work instead of
   * failing with {@link org.apache.iceberg.exceptions.NoSuchTableException} from {@link
   * Catalog#loadTable(TableIdentifier)}.
   */
  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<?> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private LoadTableResponse loadAndCommitTable(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    if (isCreate(request)) {
      logger.info("Committing staged table create for {}", ident);
      return CatalogHandlers.updateTable(catalog, ident, request);
    }
    Table table = catalog.loadTable(ident);
    if (!(table instanceof BaseTable)) {
      throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
    }
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata updated = commit(ops, request, ident);
    return LoadTableResponse.builder().withTableMetadata(updated).build();
  }

  /**
   * This is a very simplistic approach that only validates the requirements for each table and does
   * not do any other conflict detection. Therefore, it does not guarantee true transactional
   * atomicity, which is left to the implementation details of a REST server.
   */
  private void commitTransaction(CommitTransactionRequest request) {
    List<TableIdentifier> sorted =
        request.tableChanges().stream()
            .map(UpdateTableRequest::identifier)
            .distinct()
            .sorted(Comparator.comparing(TableIdentifier::toString))
            .toList();

    Runnable body = () -> commitTransactionBody(request);
    if (commitLock != null) {
      commitLock.withLocks(sorted, body);
    } else {
      body.run();
    }
  }

  private void commitTransactionBody(CommitTransactionRequest request) {
    List<Transaction> transactions = Lists.newArrayList();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      if (isCreate(tableChange)) {
        logger.info(
            "Committing staged table create (multi-table txn) for {}", tableChange.identifier());
        CatalogHandlers.updateTable(catalog, tableChange.identifier(), tableChange);
        continue;
      }

      Table table = catalog.loadTable(tableChange.identifier());
      if (table instanceof BaseTable) {
        Transaction transaction =
            Transactions.newTransaction(
                tableChange.identifier().toString(), ((BaseTable) table).operations());
        transactions.add(transaction);

        BaseTransaction.TransactionTable txTable =
            (BaseTransaction.TransactionTable) transaction.table();

        // this performs validations and makes temporary commits that are in-memory
        commit(txTable.operations(), tableChange, tableChange.identifier());
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    // only commit if validations passed previously
    transactions.forEach(Transaction::commitTransaction);
  }

  // Copied from CatalogHandlers.commit; retry budget is configurable (see CommitRetryConfig).
  private TableMetadata commit(
      TableOperations ops, UpdateTableRequest request, TableIdentifier ident) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(commitRetry.numRetries())
          .exponentialBackoff(
              commitRetry.minWaitMs(),
              commitRetry.maxWaitMs(),
              commitRetry.totalTimeoutMs(),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .onFailure(
              (task, ex) -> {
                if (ex instanceof CommitFailedException) {
                  CatalogMetrics.getInstance()
                      .recordCommitRetry(catalog.name(), namespaceLabel(ident), ident.name());
                }
              })
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
                  logger.warn(
                      "commit no-op for table {}: empty metadata changes after refresh "
                          + "(isRetry={}); client requirements validated but updates produced no changes",
                      ident,
                      isRetry.get());
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

package com.altinity.ice.rest.catalog.internal.rest;

import com.altinity.ice.rest.catalog.internal.auth.Session;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

public class RESTCatalogMiddlewareTableAWCredentials extends RESTCatalogMiddleware {

  private final Map<String, AwsCredentialsProvider> awsCredentialsProviders;

  public RESTCatalogMiddlewareTableAWCredentials(
      RESTCatalogHandler next, Map<String, AwsCredentialsProvider> awsCredentialsProviders) {
    super(next);
    this.awsCredentialsProviders = awsCredentialsProviders;
  }

  @Override
  public <T extends RESTResponse> T handle(
      Session session,
      Route route,
      Map<String, String> vars,
      Object requestBody,
      Class<T> responseType) {
    T restResponse = next.handle(session, route, vars, requestBody, responseType);
    if (restResponse instanceof LoadTableResponse loadTableResponse) {
      Map<String, String> tableConfig = loadTableResponse.config();
      applyCredentials(session, tableConfig);
    }
    return restResponse;
  }

  private void applyCredentials(Session session, Map<String, String> tableConfig) {
    String providerLookupKey = session.uid();
    AwsCredentialsProvider credentialsProvider = awsCredentialsProviders.get(providerLookupKey);
    Preconditions.checkState(credentialsProvider != null); // null here means misconfiguration
    AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
    tableConfig.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.accessKeyId());
    tableConfig.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.secretAccessKey());
    if (awsCredentials instanceof AwsSessionCredentialsIdentity) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN,
          ((AwsSessionCredentialsIdentity) awsCredentials).sessionToken());
    }
  }
}

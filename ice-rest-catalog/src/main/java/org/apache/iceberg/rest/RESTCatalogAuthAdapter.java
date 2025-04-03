package org.apache.iceberg.rest;

import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

public class RESTCatalogAuthAdapter extends RESTCatalogAdapter {

  // TODO: replace with catalog.config
  private final Map<String, String> config;
  private final AwsCredentialsProvider credentialsProvider;

  public RESTCatalogAuthAdapter(
      Catalog catalog, Map<String, String> config, AwsCredentialsProvider credentialsProvider) {
    super(catalog);
    this.config = config;
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body, Class<T> responseType) {
    T restResponse = super.handleRequest(route, vars, body, responseType);
    if (restResponse instanceof LoadTableResponse loadTableResponse) {
      applyCredentials(loadTableResponse.config());
    }
    return restResponse;
  }

  // FIXME: this is not even remotely secure
  private void applyCredentials(Map<String, String> tableConfig) {
    if (credentialsProvider != null) {
      AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
      tableConfig.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.accessKeyId());
      tableConfig.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.secretAccessKey());
      if (awsCredentials instanceof AwsSessionCredentialsIdentity) {
        tableConfig.put(
            S3FileIOProperties.SESSION_TOKEN,
            ((AwsSessionCredentialsIdentity) awsCredentials).sessionToken());
      }
    } else if (config.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
        && config.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
      tableConfig.put(
          S3FileIOProperties.ACCESS_KEY_ID, config.get(S3FileIOProperties.ACCESS_KEY_ID));
      tableConfig.put(
          S3FileIOProperties.SECRET_ACCESS_KEY, config.get(S3FileIOProperties.SECRET_ACCESS_KEY));
      if (config.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
        tableConfig.put(
            S3FileIOProperties.SESSION_TOKEN, config.get(S3FileIOProperties.SESSION_TOKEN));
      }
    }
  }
}

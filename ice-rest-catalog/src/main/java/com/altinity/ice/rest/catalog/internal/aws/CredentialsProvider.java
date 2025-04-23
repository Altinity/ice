package com.altinity.ice.rest.catalog.internal.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public final class CredentialsProvider {

  private CredentialsProvider() {}

  public static AwsCredentialsProvider assumeRule(
      AwsCredentialsProvider credentialsProvider, String roleArn, String sessionName) {
    StsClient stsClient = StsClient.builder().credentialsProvider(credentialsProvider).build();
    // TODO: durationSeconds
    AssumeRoleRequest assumeRoleRequest =
        AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(sessionName).build();
    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClient)
        .refreshRequest(assumeRoleRequest)
        .build();
  }
}

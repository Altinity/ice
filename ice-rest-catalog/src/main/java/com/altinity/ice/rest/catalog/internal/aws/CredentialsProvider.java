/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
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

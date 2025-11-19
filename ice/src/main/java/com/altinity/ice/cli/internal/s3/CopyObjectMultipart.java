/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

public class CopyObjectMultipart {

  private static final Logger logger = LoggerFactory.getLogger(CopyObjectMultipart.class);

  // S3::CopyObject has 5GiB file limit. This version doesn't.
  public static void run(S3Client s3, CopyObjectRequest req, Options opts) {
    long partSize = 256L * 1024L * 1024L; // 256 MiB

    long objectSize =
        s3.headObject(r -> r.bucket(req.sourceBucket()).key(req.sourceKey())).contentLength();

    if (objectSize < partSize) {
      logger.info(
          "Copying {}/{} to {}/{}",
          req.sourceBucket(),
          req.sourceKey(),
          req.destinationBucket(),
          req.destinationKey());

      s3.copyObject(req);
      return;
    }

    CreateMultipartUploadResponse createResponse =
        s3.createMultipartUpload(
            CreateMultipartUploadRequest.builder()
                .bucket(req.destinationBucket())
                .key(req.destinationKey())
                .build());

    String uploadId = createResponse.uploadId();
    int numberOfParts = (int) Math.ceilDiv(objectSize, partSize);

    List<Future<CompletedPart>> futures = new ArrayList<>();
    try (ExecutorService executor =
        Executors.newFixedThreadPool(opts.s3MultipartUploadThreadCount)) {
      for (int partNumber = 1; partNumber <= numberOfParts; partNumber++) {
        final int p = partNumber;
        final long rangeStart = (p - 1) * partSize;
        final long rangeEnd = Math.min(rangeStart + partSize - 1, objectSize - 1);

        futures.add(
            executor.submit(
                () -> {
                  logger.info(
                      "Copying {}/{}[{}:{}] to {}/{} (part {} of {})",
                      req.sourceBucket(),
                      req.sourceKey(),
                      rangeStart,
                      rangeEnd,
                      req.destinationBucket(),
                      req.destinationKey(),
                      p,
                      numberOfParts);

                  UploadPartCopyResponse re =
                      s3.uploadPartCopy(
                          UploadPartCopyRequest.builder()
                              .sourceBucket(req.sourceBucket())
                              .sourceKey(req.sourceKey())
                              .destinationBucket(req.destinationBucket())
                              .destinationKey(req.destinationKey())
                              .uploadId(uploadId)
                              .partNumber(p)
                              .copySourceRange("bytes=" + rangeStart + "-" + rangeEnd)
                              .build());

                  return CompletedPart.builder()
                      .partNumber(p)
                      .eTag(re.copyPartResult().eTag())
                      .build();
                }));
      }
    }

    List<CompletedPart> completedParts = new ArrayList<>();
    for (Future<CompletedPart> f : futures) {
      try {
        completedParts.add(f.get());
      } catch (InterruptedException | ExecutionException e) {
        // Cleanup.
        s3.abortMultipartUpload(
            AbortMultipartUploadRequest.builder()
                .bucket(req.destinationBucket())
                .key(req.destinationKey())
                .uploadId(uploadId)
                .build());
        throw new CompletionException(e);
      }
    }

    s3.completeMultipartUpload(
        CompleteMultipartUploadRequest.builder()
            .bucket(req.destinationBucket())
            .key(req.destinationKey())
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build());
  }

  public record Options(int s3MultipartUploadThreadCount) {

    public static Options.Builder builder() {
      return new Options.Builder();
    }

    public static final class Builder {
      private int s3MultipartUploadThreadCount;

      private Builder() {}

      public Options.Builder s3MultipartUploadThreadCount(int s3MultipartUploadThreadCount) {
        this.s3MultipartUploadThreadCount = s3MultipartUploadThreadCount;
        return this;
      }

      public Options build() {
        return new Options(s3MultipartUploadThreadCount);
      }
    }
  }
}

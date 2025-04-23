package com.altinity.ice.internal.aws;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3 {

  public static S3Client newClient(boolean noSignRequest) {
    AwsCredentialsProvider credentialsProvider;
    if (noSignRequest) {
      credentialsProvider = AnonymousCredentialsProvider.create();
    } else {
      credentialsProvider =
          AwsCredentialsProviderChain.of(
              DefaultCredentialsProvider.create(), AnonymousCredentialsProvider.create());
    }
    return S3Client.builder()
        // region is auto-resolved by default (see .region(...))
        .credentialsProvider(credentialsProvider)
        .build();
  }

  public record BucketPath(String bucket, String path) {}

  public static BucketPath bucketPath(String path) {
    var x = replacePrefix(path, "s3://", "").split("/", 2);
    if (x.length == 2) {
      return new BucketPath(x[0], x[1]);
    }
    return new BucketPath(x[0], "");
  }

  // TODO: remove; you have like 5 of these
  private static String replacePrefix(String text, String from, String to) {
    if (text.startsWith(from)) {
      return to + text.substring(from.length());
    }
    return text;
  }

  // TODO: S3FileIO::listPrefix
  public static List<String> listWildcard(S3Client s3, String bucketName, String path, int limit) {
    if (!path.contains("*")) {
      return List.of(path);
    }
    if (limit < 0) {
      limit = Integer.MAX_VALUE;
    }
    ListPattern listPattern = ListPattern.from(path);
    List<String> r = new ArrayList<>();
    String continuationToken = null;
    do {
      ListObjectsV2Request.Builder requestBuilder =
          ListObjectsV2Request.builder().bucket(bucketName).prefix(listPattern.prefix).maxKeys(100);
      if (continuationToken != null) {
        requestBuilder.continuationToken(continuationToken);
      }
      ListObjectsV2Response response = s3.listObjectsV2(requestBuilder.build());
      response.contents().stream()
          .map(S3Object::key)
          .filter(listPattern::matches)
          .limit(limit - r.size())
          .forEach(s -> r.add(String.format("s3://%s/%s", bucketName, s)));
      if (r.size() >= limit) {
        break;
      }
      continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
    } while (continuationToken != null);
    return r;
  }

  record ListPattern(String prefix, String keyPattern) {
    static ListPattern from(String path) {
      int wildcardIndex = path.indexOf('*');
      return new ListPattern(path.substring(0, wildcardIndex), path.substring(wildcardIndex));
    }

    public boolean matches(String key) {
      if (!key.startsWith(prefix)) {
        return false;
      }
      return FilenameUtils.wildcardMatch(key.substring(prefix.length()), keyPattern);
    }
  }
}

package com.altinity.ice.internal.aws;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import org.testng.annotations.Test;

public class S3Test {

  @Test
  public void testBucketPath() {
    assertEquals(S3.bucketPath("foo"), new S3.BucketPath("foo", ""));
    assertEquals(S3.bucketPath("s3://foo"), new S3.BucketPath("foo", ""));
    assertEquals(S3.bucketPath("foo/bar"), new S3.BucketPath("foo", "bar"));
    assertEquals(S3.bucketPath("s3://foo/bar"), new S3.BucketPath("foo", "bar"));
  }

  record ListPatternTestCase(
      String pattern, String[] input, String expectedPrefix, String[] expectedOutput) {}

  @Test
  public void testListPattern() {
    for (var t :
        new ListPatternTestCase[] {
          new ListPatternTestCase(
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/*.parquet",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.not-quite-parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0/not-quite-parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              },
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              }),
          new ListPatternTestCase(
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-0*/*.parquet",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-11/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              },
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-0",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              }),
          new ListPatternTestCase(
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-*/part-*.parquet",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              },
              "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-",
              new String[] {
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-0.parquet",
                "s3://aws-public-blockchain/v1.0/btc/transactions/date=2025-01-01/part-1.parquet",
              }),
        }) {
      var p = S3.ListPattern.from(t.pattern);
      assertEquals(p.prefix(), t.expectedPrefix);
      assertEquals(Arrays.stream(t.input).filter(p::matches).toArray(), t.expectedOutput);
    }
  }
}

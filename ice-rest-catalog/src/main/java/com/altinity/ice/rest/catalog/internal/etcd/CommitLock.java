/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog.internal.etcd;

import com.altinity.ice.rest.catalog.internal.config.CommitLockConfig;
import com.altinity.ice.rest.catalog.internal.metrics.CatalogMetrics;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutual exclusion for Iceberg table commits using etcd leases + the lock API (same cluster as
 * {@link EtcdCatalog}).
 */
public final class CommitLock {

  private static final Logger logger = LoggerFactory.getLogger(CommitLock.class);

  private static final StreamObserver<io.etcd.jetcd.lease.LeaseKeepAliveResponse> NOOP_KEEPALIVE =
      new StreamObserver<>() {
        @Override
        public void onNext(io.etcd.jetcd.lease.LeaseKeepAliveResponse value) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}
      };

  private final Lock lockApi;
  private final Lease leaseApi;
  private final String catalogName;
  private final CommitLockConfig config;

  public CommitLock(Client etcdClient, String catalogName, CommitLockConfig config) {
    this.lockApi = etcdClient.getLockClient();
    this.leaseApi = etcdClient.getLeaseClient();
    this.catalogName = catalogName;
    this.config = config;
  }

  /**
   * Acquire the commit lock for {@code ident}. Caller must {@link Handle#close()} to release.
   *
   * @throws CommitLockTimeoutException if the lock is not acquired within {@link
   *     CommitLockConfig#acquireTimeoutMs()}
   */
  public Handle acquire(TableIdentifier ident) {
    String path = lockPath(catalogName, ident);
    ByteSequence name = ByteSequence.from(path, StandardCharsets.UTF_8);
    long waitStartNanos = System.nanoTime();

    long leaseId;
    CloseableClient keepAlive;
    try {
      leaseId = leaseApi.grant(config.leaseTtlSeconds()).get().getID();
      keepAlive = leaseApi.keepAlive(leaseId, NOOP_KEEPALIVE);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }

    try {
      LockResponse lr =
          lockApi.lock(name, leaseId).get(config.acquireTimeoutMs(), TimeUnit.MILLISECONDS);
      long acquireEndNanos = System.nanoTime();
      CatalogMetrics.getInstance()
          .recordCommitLockAcquireSeconds(
              catalogName, (acquireEndNanos - waitStartNanos) / 1_000_000_000.0);
      return new Handle(lockApi, leaseApi, lr.getKey(), leaseId, keepAlive, acquireEndNanos);
    } catch (TimeoutException e) {
      cleanupAfterFailedAcquire(keepAlive, leaseId);
      CatalogMetrics.getInstance().recordCommitLockAcquireTimeout(catalogName);
      throw new CommitLockTimeoutException(
          "commit lock acquire timed out after " + config.acquireTimeoutMs() + " ms", e);
    } catch (InterruptedException e) {
      cleanupAfterFailedAcquire(keepAlive, leaseId);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      cleanupAfterFailedAcquire(keepAlive, leaseId);
      throw new RuntimeException(e.getCause());
    }
  }

  private void cleanupAfterFailedAcquire(CloseableClient keepAlive, long leaseId) {
    try {
      keepAlive.close();
    } catch (RuntimeException ex) {
      logger.warn("keepAlive.close failed after lock acquire failure", ex);
    }
    try {
      leaseApi.revoke(leaseId).get();
    } catch (Exception ex) {
      logger.warn("lease revoke failed after lock acquire failure", ex);
    }
  }

  /**
   * Acquire locks for every identifier in {@code sorted} order (caller must sort for deadlock-free
   * ordering), run {@code action}, then release in reverse order.
   */
  public void withLocks(List<TableIdentifier> sorted, Runnable action) {
    List<Handle> handles = new ArrayList<>(sorted.size());
    try {
      for (TableIdentifier id : sorted) {
        handles.add(acquire(id));
      }
      action.run();
    } finally {
      for (int i = handles.size() - 1; i >= 0; i--) {
        try {
          handles.get(i).close();
        } catch (RuntimeException e) {
          logger.warn("failed to release commit lock handle", e);
        }
      }
    }
  }

  static String lockPath(String catalogName, TableIdentifier ident) {
    return "locks/v1/" + catalogName + "/" + ident;
  }

  /** Lease-backed etcd lock; closes to unlock and revoke the lease. */
  public final class Handle implements AutoCloseable {

    private final Lock lockApi;
    private final Lease leaseApi;
    private final ByteSequence lockKey;
    private final long leaseId;
    private final CloseableClient keepAlive;
    private final long acquiredAtNanos;

    private Handle(
        Lock lockApi,
        Lease leaseApi,
        ByteSequence lockKey,
        long leaseId,
        CloseableClient keepAlive,
        long acquiredAtNanos) {
      this.lockApi = lockApi;
      this.leaseApi = leaseApi;
      this.lockKey = lockKey;
      this.leaseId = leaseId;
      this.keepAlive = keepAlive;
      this.acquiredAtNanos = acquiredAtNanos;
    }

    @Override
    public void close() {
      long heldNanos = System.nanoTime() - acquiredAtNanos;
      try {
        lockApi.unlock(lockKey).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      } finally {
        try {
          keepAlive.close();
        } catch (RuntimeException e) {
          logger.warn("keepAlive.close failed during unlock", e);
        }
        try {
          leaseApi.revoke(leaseId).get();
        } catch (Exception e) {
          logger.warn("lease revoke failed during unlock", e);
        }
      }
      CatalogMetrics.getInstance()
          .recordCommitLockHeldSeconds(catalogName, heldNanos / 1_000_000_000.0);
    }
  }
}

/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.rest.catalog;

import java.util.List;
import java.util.Map;

/**
 * Configuration class representing a test scenario loaded from scenario.yaml.
 *
 * <p>This class uses Jackson/SnakeYAML annotations for YAML deserialization.
 */
public class ScenarioConfig {

  private String name;
  private String description;
  private CatalogConfig catalogConfig;
  private Map<String, String> env;
  private CloudResources cloudResources;
  private List<Phase> phases;

  public static class CatalogConfig {
    private String warehouse;
    private String name;
    private String uri;

    public String getWarehouse() {
      return warehouse;
    }

    public void setWarehouse(String warehouse) {
      this.warehouse = warehouse;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getUri() {
      return uri;
    }

    public void setUri(String uri) {
      this.uri = uri;
    }
  }

  public static class CloudResources {
    private S3Resources s3;
    private SqsResources sqs;

    public S3Resources getS3() {
      return s3;
    }

    public void setS3(S3Resources s3) {
      this.s3 = s3;
    }

    public SqsResources getSqs() {
      return sqs;
    }

    public void setSqs(SqsResources sqs) {
      this.sqs = sqs;
    }
  }

  public static class S3Resources {
    private List<String> buckets;

    public List<String> getBuckets() {
      return buckets;
    }

    public void setBuckets(List<String> buckets) {
      this.buckets = buckets;
    }
  }

  public static class SqsResources {
    private List<String> queues;

    public List<String> getQueues() {
      return queues;
    }

    public void setQueues(List<String> queues) {
      this.queues = queues;
    }
  }

  public static class Phase {
    private String name;
    private String description;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }

  // Getters and setters
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public CatalogConfig getCatalogConfig() {
    return catalogConfig;
  }

  public void setCatalogConfig(CatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public void setEnv(Map<String, String> env) {
    this.env = env;
  }

  public CloudResources getCloudResources() {
    return cloudResources;
  }

  public void setCloudResources(CloudResources cloudResources) {
    this.cloudResources = cloudResources;
  }

  public List<Phase> getPhases() {
    return phases;
  }

  public void setPhases(List<Phase> phases) {
    this.phases = phases;
  }
}



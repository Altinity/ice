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

import java.util.Map;

/**
 * Configuration class representing a test scenario loaded from scenario.yaml.
 *
 * <p>This class uses Jackson/SnakeYAML annotations for YAML deserialization.
 */
public record ScenarioConfig(
    String name, String description, CatalogConfig catalogConfig, Map<String, String> env) {

  public record CatalogConfig(String warehouse, String name, String uri) {}
}

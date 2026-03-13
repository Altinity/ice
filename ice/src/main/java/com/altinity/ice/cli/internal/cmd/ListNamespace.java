/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.cmd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;

public final class ListNamespace {

  private ListNamespace() {}

  public static void run(RESTCatalog catalog, Namespace parent, boolean json) throws IOException {
    List<Namespace> namespaces = catalog.listNamespaces(parent);
    List<String> names =
        namespaces.stream().map(Namespace::toString).sorted().collect(Collectors.toList());
    var result = new Result(names);
    output(result, json);
  }

  private static void output(Result result, boolean json) throws IOException {
    ObjectMapper mapper =
        json
            ? new ObjectMapper()
            : new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    System.out.println(mapper.writeValueAsString(result));
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  record Result(List<String> namespaces) {}
}

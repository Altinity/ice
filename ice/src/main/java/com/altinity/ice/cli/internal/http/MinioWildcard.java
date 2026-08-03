/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.altinity.ice.cli.internal.http;

import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.json.*;

public final class MinioWildcard {
  private static final HttpClient client = HttpClient.newHttpClient();

  public static List<String> listHTTPWildcard(String urlWithStar) throws Exception {
    URI uri = URI.create(urlWithStar);
    String[] parts = uri.getPath().split("/", 3);
    if (parts.length < 2) throw new IllegalArgumentException("Bad MinIO URL: " + urlWithStar);

    String bucket = parts[1];
    String prefix = parts.length == 3 ? parts[2].replaceAll("\\*", "") : "";
    String endpoint = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();

    String listUrl = endpoint + "/" + bucket + "?list-type=2&prefix=" + prefix;
    HttpRequest req = HttpRequest.newBuilder(URI.create(listUrl)).GET().build();
    HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() != 200)
      throw new RuntimeException("MinIO listObjects error: " + resp.statusCode());

    JSONObject xml = org.json.XML.toJSONObject(resp.body());
    List<String> files = new ArrayList<>();
    var result = xml.getJSONObject("ListBucketResult");
    var contents = result.optJSONArray("Contents");
    if (contents != null) {
      for (int i = 0; i < contents.length(); i++) {
        String key = contents.getJSONObject(i).getString("Key");
        if (key.endsWith(".parquet")) files.add(endpoint + "/" + bucket + "/" + key);
      }
    } else {
      var single = result.optJSONObject("Contents");
      if (single != null) files.add(endpoint + "/" + bucket + "/" + single.getString("Key"));
    }
    return files;
  }
}

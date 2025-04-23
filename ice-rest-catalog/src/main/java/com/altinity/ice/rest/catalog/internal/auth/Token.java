package com.altinity.ice.rest.catalog.internal.auth;

import com.altinity.ice.rest.catalog.internal.config.Config;
import java.util.Map;

public record Token(String id, String value, Map<String, String> params) {

  public static final String TOKEN_PARAM_AWS_ASSUME_ROLE_ARN = "aws_assume_role_arn";
  public static final String TOKEN_PARAM_READ_ONLY = "ro";

  public String resourceName() {
    return id().isEmpty() ? "token" : "token:" + id();
  }

  // TODO: force min token length
  public static Token parse(String s) {
    // $id:$value:key=value&key=value&...
    String[] split = s.trim().split(":", 3);
    return switch (split.length) {
      case 1 -> new Token("", split[0], Map.of());
      case 2 -> {
        if (split[1].isEmpty()) {
          yield new Token("", split[0], Map.of());
        }
        yield new Token(split[0], split[1], Map.of());
      }
      default -> {
        Map<String, String> params = Config.parseQuery(split[2]);
        for (String key : params.keySet()) {
          if (!TOKEN_PARAM_AWS_ASSUME_ROLE_ARN.equals(key) && !TOKEN_PARAM_READ_ONLY.equals(key)) {
            throw new IllegalArgumentException(String.format("unknown token parameter %s", key));
          }
        }
        yield new Token(split[0], split[1], params);
      }
    };
  }
}

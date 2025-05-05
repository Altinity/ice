package com.altinity.ice.internal.config;

import java.io.IOException;

public class InvalidConfigException extends IOException {

  public InvalidConfigException(String message) {
    super(message);
  }
}

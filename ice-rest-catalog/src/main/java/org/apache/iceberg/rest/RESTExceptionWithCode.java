package org.apache.iceberg.rest;

import org.apache.iceberg.exceptions.RESTException;

public class RESTExceptionWithCode extends RESTException {

  private final int code;

  public RESTExceptionWithCode(int code, Throwable cause, String message, Object... args) {
    super(cause, message, args);
    this.code = code;
  }

  public int code() {
    return code;
  }
}

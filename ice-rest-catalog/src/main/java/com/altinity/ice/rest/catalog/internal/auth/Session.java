package com.altinity.ice.rest.catalog.internal.auth;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;

public record Session(String uid, Map<String, String> attrs) {

  public void applyTo(HttpServletRequest r) {
    r.setAttribute("session_uid", uid);
    r.setAttribute("session_attrs", attrs);
  }

  public static Session from(HttpServletRequest r) throws IOException {
    //noinspection unchecked
    return new Session(
        (String) r.getAttribute("session_uid"),
        (Map<String, String>) r.getAttribute("session_attrs"));
  }
}

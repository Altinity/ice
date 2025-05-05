package com.altinity.ice.rest.catalog.internal.auth;

import jakarta.servlet.http.HttpServletRequest;

public record Session(String uid, boolean readOnly, String awsAssumeRoleARN) {

  public void applyTo(HttpServletRequest r) {
    r.setAttribute("session", this);
  }

  public static Session from(HttpServletRequest r) {
    return (Session) r.getAttribute("session");
  }
}

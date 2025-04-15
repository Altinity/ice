package com.altinity.ice.rest.catalog.internal.jetty;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;

public class AuthorizationHandler extends HandlerWrapper {

  private final String expectedToken;

  public AuthorizationHandler(String expectedToken) {
    this.expectedToken = expectedToken;
  }

  @Override
  public void handle(
      String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    var auth = request.getHeader("authorization");
    String prefix = "bearer ";
    String token;
    if (auth != null && auth.toLowerCase().startsWith(prefix)) {
      token = auth.substring(prefix.length());
      if (java.security.MessageDigest.isEqual(token.getBytes(), expectedToken.getBytes())) {
        super.handle(target, baseRequest, request, response);
        return;
      }
    } else {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    response.sendError(HttpServletResponse.SC_FORBIDDEN);
    // TODO: AsyncDelayHandler
  }
}

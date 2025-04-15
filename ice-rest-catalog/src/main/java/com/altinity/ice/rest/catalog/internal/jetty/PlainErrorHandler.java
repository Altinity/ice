package com.altinity.ice.rest.catalog.internal.jetty;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;

public class PlainErrorHandler extends org.eclipse.jetty.server.handler.ErrorHandler {

  @Override
  public void handle(
      String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp)
      throws IOException, ServletException {
    resp.setStatus(resp.getStatus());
    resp.setContentType(MimeTypes.Type.TEXT_PLAIN.asString());
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    try (PrintWriter w = resp.getWriter()) {
      w.write(baseRequest.getResponse().getReason());
    } finally {
      baseRequest.getHttpChannel().sendResponseAndComplete();
      baseRequest.setHandled(true);
    }
  }
}

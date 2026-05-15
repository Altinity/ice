/*
 * Copyright (c) 2025 Altinity Inc and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.altinity.ice.cli.internal.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TreePrinter {

  private TreePrinter() {}

  public record Node(List<String> label, List<Node> children) {
    public Node(String label) {
      this(splitLines(label), List.of());
    }

    public Node(String label, List<Node> children) {
      this(splitLines(label), children);
    }

    public Node {
      label = List.copyOf(label);
      children = List.copyOf(children);
    }

    private static List<String> splitLines(String label) {
      List<String> lines = new ArrayList<>(Arrays.asList(label.split("\n", -1)));
      while (lines.size() > 1 && lines.getLast().isEmpty()) {
        lines.removeLast();
      }
      return lines;
    }
  }

  public static void print(Node root) {
    print(root, System.out);
  }

  public static void print(Node root, PrintStream out) {
    for (String line : root.label()) {
      out.println(line);
    }
    printChildren(root.children(), "", out);
  }

  private static void printChildren(List<Node> children, String descendantIndent, PrintStream out) {
    for (int i = 0; i < children.size(); i++) {
      Node child = children.get(i);
      boolean isLast = (i == children.size() - 1);
      String connector = isLast ? "└── " : "├── ";
      String childDescendantIndent = descendantIndent + (isLast ? "    " : "│   ");
      List<String> lines = child.label();
      out.println(descendantIndent + connector + lines.getFirst());
      for (int j = 1; j < lines.size(); j++) {
        out.println(childDescendantIndent + lines.get(j));
      }
      printChildren(child.children(), childDescendantIndent, out);
    }
  }
}

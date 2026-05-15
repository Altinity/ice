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
import java.util.List;

public final class TreePrinter {

  private TreePrinter() {}

  public record Node(String label, List<Node> children) {
    public Node(String label) {
      this(label, List.of());
    }

    public Node {
      children = List.copyOf(children);
    }
  }

  public static void print(Node root) {
    print(root, System.out);
  }

  public static void print(Node root, PrintStream out) {
    out.println(root.label());
    printChildren(root.children(), "", out);
  }

  private static void printChildren(List<Node> children, String descendantIndent, PrintStream out) {
    for (int i = 0; i < children.size(); i++) {
      Node child = children.get(i);
      boolean isLast = (i == children.size() - 1);
      String connector = isLast ? "└── " : "├── ";
      String childDescendantIndent = descendantIndent + (isLast ? "    " : "│   ");
      out.println(descendantIndent + connector + child.label());
      printChildren(child.children(), childDescendantIndent, out);
    }
  }
}

package com.altinity.ice.internal.picocli;

import picocli.CommandLine;

public class VersionProvider implements CommandLine.IVersionProvider {

  public String[] getVersion() {
    return new String[] {VersionProvider.class.getPackage().getImplementationVersion()};
  }
}

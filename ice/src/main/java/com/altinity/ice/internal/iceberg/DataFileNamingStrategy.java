package com.altinity.ice.internal.iceberg;

import com.altinity.ice.internal.crypto.Hash;
import org.apache.iceberg.Table;

public interface DataFileNamingStrategy {
  String get(String file);

  enum Name {
    DEFAULT,
    INPUT_FILENAME;
  }

  static String defaultDataLocation(Table table) {
    return String.format("%s/%s", table.location().replaceAll("/+$", ""), "data");
  }

  record Default(String parent, String prefix) implements DataFileNamingStrategy {

    @Override
    public String get(String file) {
      String fileName = prefix + Hash.sha256(file) + ".parquet";
      return String.format("%s/%s", parent, fileName);
    }
  }

  record InputFilename(String parent) implements DataFileNamingStrategy {

    @Override
    public String get(String file) {
      if (!file.endsWith(".parquet")) {
        throw new UnsupportedOperationException(
            "expected " + file + " to have .parquet extension"); // FIXME: remove
      }
      String fileName = file.substring(file.lastIndexOf("/") + 1);
      return String.format("%s/%s", parent, fileName);
    }
  }
}

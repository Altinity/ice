package com.altinity.ice.internal.cmd;

import java.io.IOException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeleteTable {

  private static final Logger logger = LoggerFactory.getLogger(DeleteTable.class);

  private DeleteTable() {}

  public static void run(RESTCatalog catalog, TableIdentifier nsTable, boolean ignoreNotFound)
      throws IOException {
    var purge = true; // FIXME
    if (!catalog.dropTable(nsTable, purge)) {
      if (!ignoreNotFound) {
        throw new NotFoundException(String.format("Table %s not found", nsTable));
      }
      logger.warn("Table {} not found", nsTable);
    }
  }
}

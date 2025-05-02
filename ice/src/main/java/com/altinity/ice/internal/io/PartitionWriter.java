package com.altinity.ice.internal.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.*;

public class PartitionWriter implements TaskWriter<Record> {

  private final PartitionSpec spec;
  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final Schema schema;
  private final long targetFileSize;
  private final Map<StructLike, FileAppender<Record>> openAppenders;
  private final Map<StructLike, DataFile> writtenFiles;
  private final GenericAppenderFactory appenderFactory;

  public PartitionWriter(Table table, FileFormat format, long targetFileSizeBytes) {
    this.spec = table.spec();
    this.format = format;
    this.io = table.io();
    this.schema = table.schema();
    this.targetFileSize = targetFileSizeBytes;
    this.openAppenders = new HashMap<>();
    this.writtenFiles = new HashMap<>();

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    this.appenderFactory = new GenericAppenderFactory(schema);
  }

  @Override
  public void write(Record record) throws IOException {
    PartitionKey partitionKey = new PartitionKey(spec, schema);

    partitionKey.partition(record);

    FileAppender<Record> appender = openAppenders.get(partitionKey);

    if (appender == null) {
      OutputFile file = (OutputFile) fileFactory.newOutputFile(partitionKey);
      appender = appenderFactory.newAppender(file, format);
      openAppenders.put(partitionKey, appender);
    }

    appender.add(record);
  }

  @Override
  public WriteResult complete() throws IOException {
    for (Map.Entry<StructLike, FileAppender<Record>> entry : openAppenders.entrySet()) {
      FileAppender<Record> appender = entry.getValue();
      StructLike partition = entry.getKey();

      appender.close();

      DataFile dataFile =
          DataFiles.builder(spec)
              .withPath(appender.toString())
              .withFormat(format)
              .withPartition(partition)
              .withRecordCount(appender.length())
              .withFileSizeInBytes(appender.length()) // approximation
              .build();

      writtenFiles.put(partition, dataFile);
    }

    DataFile[] dataFiles = writtenFiles.values().toArray(new DataFile[0]);
    return WriteResult.builder().addDataFiles(dataFiles).build();
  }

  @Override
  public void abort() throws IOException {
    for (FileAppender<Record> appender : openAppenders.values()) {
      appender.close();
    }
    openAppenders.clear();
  }

  @Override
  public void close() throws IOException {}
}

package com.altinity.ice.cli.internal.iceberg.parquet;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;

// org.apache.parquet.hadoop.ParquetFileReader but without Hadoop dependency
// TODO: refactor
public final class Metadata {

  private Metadata() {}

  public static ParquetMetadata read(InputFile inputFile) throws IOException {
    try (SeekableInputStream s = inputFile.newStream()) {
      DelegatingSeekableInputStream ds =
          new DelegatingSeekableInputStream(s) {

            @Override
            public long getPos() throws IOException {
              return s.getPos();
            }

            @Override
            public void seek(long newPos) throws IOException {
              s.seek(newPos);
            }
          };
      return readFooter(inputFile, ds, new ParquetMetadataConverter());
    }
  }

  // copied from org.apache.parquet.hadoop.ParquetFileReader
  private static ParquetMetadata readFooter(
      org.apache.iceberg.io.InputFile file,
      DelegatingSeekableInputStream f,
      ParquetMetadataConverter converter)
      throws IOException {

    long fileLen = file.getLength();
    String filePath = file.toString();

    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen
        < MAGIC.length
            + FOOTER_LENGTH_SIZE
            + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(
          filePath + " is not a Parquet file (length is too low: " + fileLen + ")");
    }

    // Read footer length and magic string - with a single seek
    byte[] magic = new byte[MAGIC.length];
    long fileMetadataLengthIndex = fileLen - magic.length - FOOTER_LENGTH_SIZE;
    f.seek(fileMetadataLengthIndex);
    int fileMetadataLength = readIntLittleEndian(f);
    f.readFully(magic);

    if (Arrays.equals(EFMAGIC, magic)) {
      throw new ParquetCryptoRuntimeException(
          "Parquet files with encrypted footer are currently not supported");
    } else if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(
          filePath
              + " is not a Parquet file. Expected magic number at tail, but found "
              + Arrays.toString(magic));
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException(
          "corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    f.seek(fileMetadataIndex);

    var allocator = new HeapByteBufferAllocator(); // FIXME: move it out of here
    ByteBuffer footerBytesBuffer = allocator.allocate(fileMetadataLength);
    try {
      f.readFully(footerBytesBuffer);
      footerBytesBuffer.flip();
      InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);

      ParquetMetadataConverter.MetadataFilter metadataFilter =
          ParquetMetadataConverter.NO_FILTER; // options.getMetadataFilter()
      return converter.readParquetMetadata(
          footerBytesStream, metadataFilter, null, false, fileMetadataLength);
    } finally {
      allocator.release(footerBytesBuffer);
    }
  }
}

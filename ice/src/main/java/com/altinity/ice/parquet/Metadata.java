package com.altinity.ice.parquet;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.format.Util.readFileCryptoMetaData;
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
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.FileCryptoMetaData;
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
    //        LOG.debug("File length {}", fileLen);

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
    //        LOG.debug("reading footer index at {}", fileMetadataLengthIndex);
    f.seek(fileMetadataLengthIndex);
    int fileMetadataLength = readIntLittleEndian(f);
    //        f.readNBytes(magic, 0, magic.length); // FIXME: check how may bytes read
    f.readFully(magic);

    boolean encryptedFooterMode;
    if (Arrays.equals(MAGIC, magic)) {
      encryptedFooterMode = false;
    } else if (Arrays.equals(EFMAGIC, magic)) {
      encryptedFooterMode = true;
    } else {
      throw new RuntimeException(
          filePath
              + " is not a Parquet file. Expected magic number at tail, but found "
              + Arrays.toString(magic));
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    //        LOG.debug("read footer length: {}, footer index: {}", fileMetadataLength,
    // fileMetadataIndex);
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException(
          "corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    f.seek(fileMetadataIndex);

    FileDecryptionProperties fileDecryptionProperties = null; // options.getDecryptionProperties();
    InternalFileDecryptor fileDecryptor = null;
    if (null != fileDecryptionProperties) {
      fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
    }

    // options.getAllocator()
    var allocator = new HeapByteBufferAllocator(); // FIXME: move it out of here
    // Read all the footer bytes in one time to avoid multiple read operations,
    // since it can be pretty time consuming for a single read operation in HDFS.
    ByteBuffer footerBytesBuffer = allocator.allocate(fileMetadataLength);
    try {
      f.readFully(footerBytesBuffer);
      //            f.readNBytes(footerBytesBuffer.length); // FIXME: check how may bytes read
      //            LOG.debug("Finished to read all footer bytes.");
      footerBytesBuffer.flip();
      InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);

      ParquetMetadataConverter.MetadataFilter metadataFilter =
          ParquetMetadataConverter.NO_FILTER; // options.getMetadataFilter()

      // Regular file, or encrypted file with plaintext footer
      if (!encryptedFooterMode) {
        return converter.readParquetMetadata(
            footerBytesStream, metadataFilter, fileDecryptor, false, fileMetadataLength);
      }

      // Encrypted file with encrypted footer
      if (null == fileDecryptor) {
        throw new ParquetCryptoRuntimeException(
            "Trying to read file with encrypted footer. No keys available");
      }
      FileCryptoMetaData fileCryptoMetaData = readFileCryptoMetaData(footerBytesStream);
      fileDecryptor.setFileCryptoMetaData(
          fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
      // footer length is required only for signed plaintext footers
      return converter.readParquetMetadata(
          footerBytesStream, metadataFilter, fileDecryptor, true, 0);
    } finally {
      allocator.release(footerBytesBuffer);
    }
  }
}

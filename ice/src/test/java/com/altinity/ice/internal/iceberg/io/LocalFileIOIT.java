package com.altinity.ice.internal.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.altinity.ice.internal.strings.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LocalFileIOIT {

  private Path tempDir;

  @BeforeMethod
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("testng-temp-");
    System.out.println("Created temp dir: " + tempDir);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      try (var t = Files.walk(tempDir)) {
        t.sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(
                file -> {
                  if (!file.delete()) {
                    System.err.println("Failed to delete: " + file);
                  }
                });
      }
      System.out.println("Deleted temp dir: " + tempDir);
    }
  }

  @Test
  public void testBasic() throws IOException {
    for (var warehouse : new String[] {"file://.", "file://", "file://x/y/z"}) {
      tempDir.toFile().mkdirs();
      new File(tempDir.toString(), Strings.removePrefix(warehouse, "file://")).mkdirs();
      Path fooFile = tempDir.resolve(Strings.removePrefix(warehouse, "file://")).resolve("foo");
      Files.writeString(fooFile, "foo_content");
      Files.writeString(
          tempDir.resolve(Strings.removePrefix(warehouse, "file://")).resolve("bar"),
          "bar_content");
      try (LocalFileIO io = new LocalFileIO()) {
        assertThatThrownBy(
                () ->
                    io.initialize(
                        Map.of(
                            LocalFileIO.LOCALFILEIO_PROP_BASEDIR,
                            fooFile.toRealPath().toString(),
                            LocalFileIO.LOCALFILEIO_PROP_WAREHOUSE,
                            warehouse)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must point to an existing directory");

        /*
                assertThatThrownBy(
                        () ->
                            io.initialize(Map.of(LocalFileIO.LOCALFILEIO_PROP_BASEDIR, tempDir.toString())))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("required");
        */

        Function<String, String> warehouseLocation =
            (String s) -> (warehouse.endsWith("/") ? warehouse : warehouse + "/") + s;

        io.initialize(
            Map.of(
                LocalFileIO.LOCALFILEIO_PROP_BASEDIR,
                tempDir.toString(),
                LocalFileIO.LOCALFILEIO_PROP_WAREHOUSE,
                warehouse));

        InputFile inputFile = io.newInputFile("foo");
        OutputFile outputFile = io.newOutputFile("foo.out");
        try (var d = outputFile.create()) {
          try (var s = inputFile.newStream()) {
            s.transferTo(d);
          }
        }
        try (var s = io.newInputFile("foo.out").newStream()) {
          assertThat(new String(s.readAllBytes())).isEqualTo("foo_content");
        }
        assertThat(io.newInputFile("baz/file.out").location())
            .isEqualTo(warehouseLocation.apply("baz/file.out"));
        assertThat(io.newOutputFile("baz/file.out").toInputFile().location())
            .isEqualTo(warehouseLocation.apply("baz/file.out"));
        assertThat(io.newOutputFile("baz/file.out").location())
            .isEqualTo(warehouseLocation.apply("baz/file.out"));
        assertThat(
                StreamSupport.stream(io.listPrefix("").spliterator(), false)
                    .map(FileInfo::location)
                    .sorted()
                    .toList())
            .isEqualTo(
                List.of(
                    warehouseLocation.apply("bar"),
                    warehouseLocation.apply("foo"),
                    warehouseLocation.apply("foo.out")));
        assertThat(
                StreamSupport.stream(io.listPrefix("foo").spliterator(), false)
                    .map(FileInfo::location)
                    .sorted()
                    .toList())
            .isEqualTo(List.of(warehouseLocation.apply("foo")));
        assertThatThrownBy(() -> io.listPrefix("..")).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> io.listPrefix("/")).isInstanceOf(SecurityException.class);
        io.deleteFiles(List.of("foo.out"));
        assertThat(
                StreamSupport.stream(io.listPrefix("").spliterator(), false)
                    .map(FileInfo::location)
                    .sorted()
                    .toList())
            .isEqualTo(List.of(warehouseLocation.apply("bar"), warehouseLocation.apply("foo")));
        io.deletePrefix("foo");
        assertThat(
                StreamSupport.stream(io.listPrefix("").spliterator(), false)
                    .map(FileInfo::location)
                    .sorted()
                    .toList())
            .isEqualTo(List.of(warehouseLocation.apply("bar")));
        io.deletePrefix("");
        assertThat(
                StreamSupport.stream(io.listPrefix("").spliterator(), false)
                    .map(FileInfo::location)
                    .sorted()
                    .toList())
            .isEqualTo(List.of());
      }
    }
  }

  /*  @Test
  public void testWithCatalog() throws IOException {
    new InMemoryCatalog();

  }*/
}

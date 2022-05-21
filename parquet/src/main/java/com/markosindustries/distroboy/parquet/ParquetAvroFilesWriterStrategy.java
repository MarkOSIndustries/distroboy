package com.markosindustries.distroboy.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

public class ParquetAvroFilesWriterStrategy<I> extends ParquetFilesWriteStrategy<I> {
  public ParquetAvroFilesWriterStrategy(Function<I, Path> pathProvider, Class<I> rowClass) {
    super(
        pathProvider,
        (path) -> {
          try {
            return ParquetAvro.parquetAvroWriter(
                new SimpleOutputFile(new File(path.toAbsolutePath().toString())), rowClass);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

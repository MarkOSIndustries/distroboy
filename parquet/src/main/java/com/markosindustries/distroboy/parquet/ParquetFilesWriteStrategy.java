package com.markosindustries.distroboy.parquet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetFilesWriteStrategy<I> implements WriterStrategy<I, Path> {
  private final Function<I, Path> pathProvider;
  private final Function<Path, ParquetWriter<I>> newParquetWriter;
  private final Map<Path, ParquetWriter<I>> pathWriters;

  public ParquetFilesWriteStrategy(
      Function<I, Path> pathProvider, Function<Path, ParquetWriter<I>> newParquetWriter) {
    this.pathProvider = pathProvider;
    this.newParquetWriter = newParquetWriter;
    this.pathWriters = new HashMap<>();
  }

  @Override
  public ParquetWriter<I> writerFor(I input) {
    return pathWriters.computeIfAbsent(pathProvider.apply(input), newParquetWriter);
  }

  @Override
  public Collection<Path> getResults() {
    return pathWriters.keySet();
  }

  @Override
  public void closeAll() {
    pathWriters.forEach(
        (path, writer) -> {
          try {
            writer.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

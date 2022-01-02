package com.markosindustries.distroboy.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetAvroFilesWriterStrategy<I> implements WriterStrategy<I, Path> {
  private final Map<Path, ParquetWriter<I>> pathWriters;
  private final Function<I, Path> pathProvider;
  private final Class<I> rowClass;

  public ParquetAvroFilesWriterStrategy(Function<I, Path> pathProvider, Class<I> rowClass) {
    this.pathProvider = pathProvider;
    this.rowClass = rowClass;
    this.pathWriters = new HashMap<>();
  }

  @Override
  public ParquetWriter<I> writerFor(I input) {
    return pathWriters.computeIfAbsent(
        pathProvider.apply(input),
        (path) -> {
          try {
            return ParquetAvro.parquetAvroWriter(
                new SimpleOutputFile(new File(path.toAbsolutePath().toString())), rowClass);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
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

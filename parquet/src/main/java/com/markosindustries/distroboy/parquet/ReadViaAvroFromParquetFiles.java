package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.File;
import java.nio.file.Path;
import org.apache.parquet.io.InputFile;

public class ReadViaAvroFromParquetFiles<O> implements FlatMapOp<Path, O> {
  private final ReadViaAvroFromParquet<InputFile, O> wrapped;

  public ReadViaAvroFromParquetFiles(Class<O> rowClass) {
    this.wrapped = new ReadViaAvroFromParquet<>(rowClass);
  }

  @Override
  public IteratorWithResources<O> flatMap(Path input) {
    return wrapped.flatMap(new SimpleInputFile(new File(input.toAbsolutePath().toString())));
  }
}

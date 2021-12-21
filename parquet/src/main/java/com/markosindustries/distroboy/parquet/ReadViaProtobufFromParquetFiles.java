package com.markosindustries.distroboy.parquet;

import com.google.protobuf.Message;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.File;
import java.nio.file.Path;

public class ReadViaProtobufFromParquetFiles<O extends Message> implements FlatMapOp<Path, O> {
  @Override
  public IteratorWithResources<O> flatMap(Path input) {
    return new ReadViaProtobufFromParquet<SimpleInputFile, O>()
        .flatMap(new SimpleInputFile(new File(input.toAbsolutePath().toString())));
  }
}
package com.markosindustries.distroboy.parquet;

import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

public class ParquetProtobufFilesWriterStrategy<I extends Message>
    extends ParquetFilesWriteStrategy<I> {
  public ParquetProtobufFilesWriterStrategy(Function<I, Path> pathProvider, Class<I> rowClass) {
    super(
        pathProvider,
        (path) -> {
          try {
            return ParquetProtobuf.parquetProtobufWriter(
                new SimpleOutputFile(new File(path.toAbsolutePath().toString())), rowClass);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

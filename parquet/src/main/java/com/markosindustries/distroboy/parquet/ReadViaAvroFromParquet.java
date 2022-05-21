package com.markosindustries.distroboy.parquet;

import java.io.IOException;
import org.apache.parquet.io.InputFile;

public class ReadViaAvroFromParquet<I extends InputFile, O> extends ReadObjectsFromParquet<I, O> {
  public ReadViaAvroFromParquet(Class<O> rowClass) {
    super(
        input -> {
          try {
            return ParquetAvro.parquetAvroReader(input, rowClass);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

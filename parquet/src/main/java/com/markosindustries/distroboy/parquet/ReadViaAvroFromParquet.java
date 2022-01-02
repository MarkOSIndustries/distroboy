package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

public class ReadViaAvroFromParquet<I extends InputFile, O> implements FlatMapOp<I, O> {
  private final Class<O> rowClass;

  public ReadViaAvroFromParquet(Class<O> rowClass) {
    this.rowClass = rowClass;
  }

  @Override
  public IteratorWithResources<O> flatMap(I input) {
    try {
      ParquetReader<O> reader = ParquetAvro.parquetAvroReader(input, rowClass);

      return new IteratorWithResources<O>() {
        O next = reader.read();

        @Override
        public void close() throws Exception {
          reader.close();
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public O next() {
          O result = next;
          try {
            next = reader.read();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

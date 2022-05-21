package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import java.util.function.Function;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

public class ReadObjectsFromParquet<I extends InputFile, O> implements FlatMapOp<I, O> {
  private final Function<I, ParquetReader<O>> newParquetReader;

  public ReadObjectsFromParquet(Function<I, ParquetReader<O>> newParquetReader) {
    this.newParquetReader = newParquetReader;
  }

  @Override
  public IteratorWithResources<O> flatMap(I input) {
    try {
      return new IteratorWithResources<O>() {
        final ParquetReader<O> reader = newParquetReader.apply(input);
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

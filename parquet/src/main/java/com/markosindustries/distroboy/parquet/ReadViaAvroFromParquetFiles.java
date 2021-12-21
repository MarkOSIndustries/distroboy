package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ReadViaAvroFromParquetFiles<O> implements FlatMapOp<Path, O> {
  private final Class<O> recordClass;

  public ReadViaAvroFromParquetFiles(Class<O> recordClass) {
    this.recordClass = recordClass;
  }

  @Override
  public IteratorWithResources<O> flatMap(Path input) {
    try {
      ParquetReader<O> reader =
          AvroParquetReader.<O>builder(
                  new SimpleInputFile(new File(input.toAbsolutePath().toString())))
              .withDataModel(new ReflectData(recordClass.getClassLoader()))
              .disableCompatibility()
              .build();

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

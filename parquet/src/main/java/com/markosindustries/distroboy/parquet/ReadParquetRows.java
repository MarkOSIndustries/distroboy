package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.FlatMappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import org.apache.parquet.io.InputFile;

public class ReadParquetRows implements FlatMapOp<InputFile, ParquetGroupInspector> {
  @Override
  public IteratorWithResources<ParquetGroupInspector> flatMap(InputFile input) {
    try {
      final var rowGroupIterator = new ParquetRowGroupIterator(input);
      return new FlatMappingIteratorWithResources<>(
          rowGroupIterator,
          rowGroup -> new ParquetRowIterator(rowGroupIterator.getSchema(), rowGroup));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

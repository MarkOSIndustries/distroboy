package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.FlatMappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadParquetRowsFromFiles implements FlatMapOp<Path, ParquetGroupInspector> {
  @Override
  public IteratorWithResources<ParquetGroupInspector> flatMap(Path input) {
    try {
      final var rowGroupIterator =
          new ParquetFileRowGroupIterator(new File(input.toAbsolutePath().toString()));
      return new FlatMappingIteratorWithResources<>(
          rowGroupIterator,
          rowGroup -> new ParquetRowIterator(rowGroupIterator.getSchema(), rowGroup));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

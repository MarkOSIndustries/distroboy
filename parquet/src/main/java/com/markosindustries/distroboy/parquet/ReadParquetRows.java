package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.FlatMappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ReadParquetRows<I extends InputFile> implements FlatMapOp<I, ParquetGroupInspector> {
  private final Function<MessageType, MessageType> projectSchema;

  public ReadParquetRows() {
    this(Function.identity());
  }

  public ReadParquetRows(MessageType projectedSchema) {
    this(ignored -> projectedSchema);
  }

  public ReadParquetRows(Function<MessageType, MessageType> projectSchema) {
    this.projectSchema = projectSchema;
  }

  @Override
  public IteratorWithResources<ParquetGroupInspector> flatMap(I input) {
    try {
      final var rowGroupIterator = new ParquetRowGroupIterator(input, projectSchema);
      return new FlatMappingIteratorWithResources<>(
          rowGroupIterator, rowGroupIterator::rowIterator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

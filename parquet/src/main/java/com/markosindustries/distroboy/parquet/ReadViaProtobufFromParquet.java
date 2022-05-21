package com.markosindustries.distroboy.parquet;

import com.google.protobuf.Message;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import org.apache.parquet.io.InputFile;

public class ReadViaProtobufFromParquet<I extends InputFile, O extends Message>
    implements FlatMapOp<I, O> {
  private final ReadViaProtobufBuildersFromParquet<InputFile, O> builderReader;

  public ReadViaProtobufFromParquet() {
    this.builderReader = new ReadViaProtobufBuildersFromParquet<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public IteratorWithResources<O> flatMap(final I input) {
    return new MappingIteratorWithResources<>(
        builderReader.flatMap(input), builder -> (O) builder.build());
  }
}

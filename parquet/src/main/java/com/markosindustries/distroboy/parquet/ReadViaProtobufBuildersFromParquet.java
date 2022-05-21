package com.markosindustries.distroboy.parquet;

import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoParquetReader;

public class ReadViaProtobufBuildersFromParquet<I extends InputFile, O extends Message>
    extends ReadObjectsFromParquet<I, Message.Builder> {
  public ReadViaProtobufBuildersFromParquet() {
    super(
        (input) -> {
          try {
            return ProtoParquetReader.<O.Builder>builder(input).build();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

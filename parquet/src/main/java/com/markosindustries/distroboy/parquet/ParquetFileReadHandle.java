package com.markosindustries.distroboy.parquet;

import java.io.File;
import java.io.IOException;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

public class ParquetFileReadHandle implements AutoCloseable {
  private final ParquetFileReader reader;
  private final MessageType schema;

  public ParquetFileReadHandle(File file) throws IOException {
    this.reader =
        ParquetFileReader.open(new SimpleInputFile(file), ParquetReadOptions.builder().build());
    this.schema = reader.getFooter().getFileMetaData().getSchema();
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}

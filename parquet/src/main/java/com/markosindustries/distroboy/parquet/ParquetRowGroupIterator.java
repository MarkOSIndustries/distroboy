package com.markosindustries.distroboy.parquet;

import static java.util.Objects.nonNull;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.io.IOException;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetRowGroupIterator implements IteratorWithResources<PageReadStore> {
  private final ParquetFileReader reader;
  private final MessageType schema;
  private PageReadStore currentPage;

  // TODO: support selecting a subset of columns, predicate pushdown via row filtering, etc (via
  //  read options)
  public ParquetRowGroupIterator(InputFile inputFile) throws IOException {
    this.reader = ParquetFileReader.open(inputFile, ParquetReadOptions.builder().build());
    this.schema = reader.getFooter().getFileMetaData().getSchema();
    currentPage = reader.readNextRowGroup();
  }

  @Override
  public boolean hasNext() {
    return nonNull(currentPage);
  }

  @Override
  public PageReadStore next() {
    final var result = currentPage;
    try {
      currentPage = reader.readNextRowGroup();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }

  public MessageType getSchema() {
    return schema;
  }
}

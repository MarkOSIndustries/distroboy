package com.markosindustries.distroboy.parquet;

import static java.util.Objects.nonNull;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.io.IOException;
import java.util.function.Function;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetRowGroupIterator implements IteratorWithResources<PageReadStore> {
  private final ParquetFileReader reader;
  private final MessageType schema;
  private PageReadStore currentPage;

  // TODO: support predicate pushdown via row filtering (via
  //  read options)
  public ParquetRowGroupIterator(InputFile inputFile) throws IOException {
    this(inputFile, Function.identity());
  }

  public ParquetRowGroupIterator(InputFile inputFile, MessageType projection) throws IOException {
    this(inputFile, ignored -> projection);
  }

  public ParquetRowGroupIterator(
      InputFile inputFile, Function<MessageType, MessageType> projectSchema) throws IOException {
    this(ParquetFileReader.open(inputFile), projectSchema);
  }

  public ParquetRowGroupIterator(
      ParquetFileReader reader, Function<MessageType, MessageType> projectSchema)
      throws IOException {
    this.reader = reader;
    this.schema = projectSchema.apply(reader.getFooter().getFileMetaData().getSchema());
    reader.setRequestedSchema(schema);
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

  public ParquetRowIterator rowIterator(PageReadStore rowGroup) {
    return new ParquetRowIterator(schema, rowGroup);
  }
}

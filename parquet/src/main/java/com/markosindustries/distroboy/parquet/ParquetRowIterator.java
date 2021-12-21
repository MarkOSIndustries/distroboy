package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

public class ParquetRowIterator implements IteratorWithResources<ParquetGroupInspector> {
  private final MessageType schema;
  private final PageReadStore rowGroup;
  private final RecordReader<Group> recordReader;
  private final long totalRows;
  private long currentRow;

  // TODO: support "shouldSkipCurrentRecord"
  public ParquetRowIterator(MessageType schema, PageReadStore rowGroup) {
    this.schema = schema;
    this.rowGroup = rowGroup;
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    this.recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
    this.totalRows = rowGroup.getRowCount();
    this.currentRow = 0;
  }

  @Override
  public boolean hasNext() {
    return currentRow < totalRows;
  }

  @Override
  public ParquetGroupInspector next() {
    currentRow++;
    return new ParquetGroupInspector(recordReader.read());
  }

  @Override
  public void close() throws Exception {}
}

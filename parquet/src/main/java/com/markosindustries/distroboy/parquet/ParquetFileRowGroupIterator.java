package com.markosindustries.distroboy.parquet;

import java.io.File;
import java.io.IOException;

public class ParquetFileRowGroupIterator extends ParquetRowGroupIterator {
  // TODO: support selecting a subset of columns, predicate pushdown via row filtering, etc (via
  //  read options)
  public ParquetFileRowGroupIterator(File file) throws IOException {
    super(new SimpleInputFile(file));
  }
}

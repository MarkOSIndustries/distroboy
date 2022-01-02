package com.markosindustries.distroboy.parquet;

import java.util.Collection;
import org.apache.parquet.hadoop.ParquetWriter;

public interface WriterStrategy<I, O> {
  ParquetWriter<I> writerFor(I input);

  Collection<O> getResults();

  void closeAll();
}

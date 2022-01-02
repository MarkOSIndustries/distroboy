package com.markosindustries.distroboy.parquet;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.ReduceOp;
import java.util.ArrayList;
import java.util.List;

public class WriteToParquet<I, O> implements ReduceOp<I, List<O>> {
  private final WriterStrategy<I, O> writerStrategy;

  public WriteToParquet(WriterStrategy<I, O> writerStrategy) {
    this.writerStrategy = writerStrategy;
  }

  @Override
  public IteratorWithResources<List<O>> asIterator(List<O> aggregate) {
    writerStrategy.closeAll();

    return ReduceOp.super.asIterator(new ArrayList<>(writerStrategy.getResults()));
  }

  @Override
  public List<O> initAggregate() {
    return new ArrayList<>();
  }

  @Override
  public List<O> reduceInput(List<O> aggregate, I input) throws Exception {
    writerStrategy.writerFor(input).write(input);
    return aggregate;
  }

  @Override
  public List<O> reduceOutput(List<O> aggregate, List<O> result) {
    aggregate.addAll(result);
    return aggregate;
  }
}

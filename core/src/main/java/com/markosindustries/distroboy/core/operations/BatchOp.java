package com.markosindustries.distroboy.core.operations;

import com.google.common.collect.Iterators;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.List;

public class BatchOp<I> implements ListOp<I, List<I>> {
  private final int batchSize;

  public BatchOp(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public IteratorWithResources<List<I>> apply(IteratorWithResources<I> input) throws Exception {
    return IteratorWithResources.from(Iterators.partition(input, batchSize), input);
  }
}

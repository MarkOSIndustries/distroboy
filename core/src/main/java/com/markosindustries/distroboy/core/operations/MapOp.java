package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;

@FunctionalInterface
public interface MapOp<I, O> extends ListOp<I, O> {
  O map(I input);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    return new MappingIteratorWithResources<>(input, this::map);
  }
}

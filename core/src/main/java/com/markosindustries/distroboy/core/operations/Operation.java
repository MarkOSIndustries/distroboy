package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

public interface Operation<I, O, C> {
  IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception;

  C collect(Iterator<O> results);
}

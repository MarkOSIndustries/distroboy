package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

/**
 * A high level interface for any operation which takes a distributed data set and runs the same
 * {@link #forEach} loop on each node against its local fragment of the data set. If you need to
 * then run more operations or collect some aggregate from each node, use {@link MapOp} instead.
 *
 * @param <I> The type of the input data set items
 */
public interface ForEachOp<I> extends Operation<I, Void, Void> {
  /**
   * Do something with the given value
   *
   * @param input The value to perform some action with
   */
  void forEach(I input);

  @Override
  default IteratorWithResources<Void> apply(IteratorWithResources<I> input) throws Exception {
    while (input.hasNext()) {
      forEach(input.next());
    }
    return IteratorWithResources.emptyIterator();
  }

  @Override
  default Void collect(Iterator<Void> results) {
    while (results.hasNext()) {
      results.next();
    }
    return null;
  }
}

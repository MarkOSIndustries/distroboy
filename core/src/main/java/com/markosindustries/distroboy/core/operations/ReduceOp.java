package com.markosindustries.distroboy.core.operations;

import static java.util.Objects.nonNull;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A high level interface for any operation which takes a distributed data set and reduces it down
 * to a single aggregate
 *
 * @param <I> The type of the input data set items
 * @param <O> The type of the output data set items
 */
public interface ReduceOp<I, O> extends Operation<I, O, O> {
  default O initAggregate() {
    return null;
  }

  default IteratorWithResources<O> asIterator(O aggregate) {
    return IteratorWithResources.from(
        nonNull(aggregate) ? List.of(aggregate).iterator() : Collections.emptyIterator());
  }

  O reduceInput(O aggregate, I input) throws Exception;

  O reduceOutput(O aggregate, O result);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    try (input) {
      var aggregate = initAggregate();
      while (input.hasNext()) {
        aggregate = reduceInput(aggregate, input.next());
      }
      return asIterator(aggregate);
    }
  }

  @Override
  default O collect(Iterator<O> results) {
    var aggregate = initAggregate();
    while (results.hasNext()) {
      aggregate = reduceOutput(aggregate, results.next());
    }
    return aggregate;
  }
}

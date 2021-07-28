package distroboy.core.operations;

import static java.util.Objects.nonNull;

import distroboy.core.iterators.IteratorWithResources;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

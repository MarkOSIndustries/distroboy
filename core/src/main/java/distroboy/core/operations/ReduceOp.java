package distroboy.core.operations;

import static java.util.Objects.nonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface ReduceOp<I, O> extends Operation<I, O, O> {
  default O initAggregate() {
    return null;
  }

  default Iterator<O> asIterator(O aggregate) {
    return nonNull(aggregate) ? List.of(aggregate).iterator() : Collections.emptyIterator();
  }

  O reduceInput(O aggregate, I input);

  O reduceOutput(O aggregate, O result);

  @Override
  default Iterator<O> apply(Iterator<I> input) {
    var aggregate = initAggregate();
    while (input.hasNext()) {
      aggregate = reduceInput(aggregate, input.next());
    }
    return asIterator(aggregate);
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

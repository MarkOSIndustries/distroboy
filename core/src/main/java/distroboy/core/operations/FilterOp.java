package distroboy.core.operations;

import distroboy.core.iterators.FilteringIteratorWithResources;
import distroboy.core.iterators.IteratorWithResources;

public interface FilterOp<I> extends ListOp<I, I> {
  boolean filter(I input);

  @Override
  default IteratorWithResources<I> apply(IteratorWithResources<I> input) {
    if (!input.hasNext()) {
      return input;
    }

    return new FilteringIteratorWithResources<>(input, this::filter);
  }
}

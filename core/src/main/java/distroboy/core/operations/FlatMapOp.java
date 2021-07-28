package distroboy.core.operations;

import distroboy.core.iterators.FlatMappingIteratorWithResources;
import distroboy.core.iterators.IteratorWithResources;

public interface FlatMapOp<I, O> extends ListOp<I, O> {
  IteratorWithResources<O> flatMap(I input);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    return new FlatMappingIteratorWithResources<>(input, this::flatMap);
  }
}

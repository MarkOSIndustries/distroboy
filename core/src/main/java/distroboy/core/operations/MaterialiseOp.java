package distroboy.core.operations;

import distroboy.core.iterators.IteratorTo;
import distroboy.core.iterators.IteratorWithResources;

public class MaterialiseOp<I> implements ListOp<I, I> {
  @Override
  public IteratorWithResources<I> apply(IteratorWithResources<I> input) throws Exception {
    return IteratorWithResources.from(IteratorTo.list(input).iterator());
  }
}

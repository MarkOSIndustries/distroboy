package distroboy.core.operations;

import java.util.Iterator;

public class MaterialiseOp<I> implements ListOp<I, I> {
  @Override
  public Iterator<I> apply(Iterator<I> input) {
    return ListFrom.iterator(input).iterator();
  }
}

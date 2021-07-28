package distroboy.core.operations;

import distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

public interface ForEachOp<I> extends Operation<I, Void, Void> {
  void forEach(I input);

  @Override
  default IteratorWithResources<Void> apply(IteratorWithResources<I> input) throws Exception {
    return IteratorWithResources.emptyIterator();
  }

  @Override
  default Void collect(Iterator<Void> results) {
    return null;
  }
}

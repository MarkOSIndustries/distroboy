package distroboy.core.operations;

import distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

public interface Operation<I, O, C> {
  IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception;

  C collect(Iterator<O> results);
}

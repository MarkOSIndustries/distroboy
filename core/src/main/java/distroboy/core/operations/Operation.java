package distroboy.core.operations;

import java.util.Iterator;

public interface Operation<I, O, C> {
  Iterator<O> apply(Iterator<I> input);

  C collect(Iterator<O> results);
}

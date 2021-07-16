package distroboy.core.operations;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

public class FlatMapIterator<I, O> implements Iterator<O> {
  private final Iterator<I> input;
  private final Function<I, Iterator<O>> flatten;
  private Iterator<O> current = Collections.emptyIterator();

  public FlatMapIterator(Iterator<I> input, Function<I, Iterator<O>> flatten) {
    this.input = input;
    this.flatten = flatten;
  }

  private void ensureCurrentHasNext() {
    while (!current.hasNext() && input.hasNext()) {
      current = flatten.apply(input.next());
    }
  }

  @Override
  public boolean hasNext() {
    ensureCurrentHasNext();
    return current.hasNext();
  }

  @Override
  public O next() {
    ensureCurrentHasNext();
    return current.next();
  }
}

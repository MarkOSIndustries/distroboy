package distroboy.core.operations;

import java.util.Iterator;
import java.util.function.Function;

public class MappingIterator<I, O> implements Iterator<O> {
  private final Iterator<I> wrapped;
  private final Function<I, O> mapper;

  public MappingIterator(Iterator<I> wrapped, Function<I, O> mapper) {
    this.wrapped = wrapped;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return wrapped.hasNext();
  }

  @Override
  public O next() {
    return mapper.apply(wrapped.next());
  }
}

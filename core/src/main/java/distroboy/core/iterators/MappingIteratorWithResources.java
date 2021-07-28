package distroboy.core.iterators;

import java.util.function.Function;

public class MappingIteratorWithResources<I, O> implements IteratorWithResources<O> {
  private final IteratorWithResources<I> wrapped;
  private final Function<I, O> mapper;

  public MappingIteratorWithResources(IteratorWithResources<I> wrapped, Function<I, O> mapper) {
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

  @Override
  public void close() throws Exception {
    wrapped.close();
  }
}

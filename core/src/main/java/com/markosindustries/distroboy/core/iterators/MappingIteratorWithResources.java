package com.markosindustries.distroboy.core.iterators;

import static java.util.Collections.unmodifiableList;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * An {@link IteratorWithResources} which will take elements in the wrapped iterator and apply a map
 * operation to them
 *
 * @param <I> The type of elements in the input {@link IteratorWithResources}
 * @param <O> The type of elements in the output {@link IteratorWithResources}
 */
public class MappingIteratorWithResources<I, O> implements IteratorWithResources<O> {
  private final IteratorWithResources<I> wrapped;
  private final Function<I, O> mapper;
  private final List<AutoCloseable> resources;

  public MappingIteratorWithResources(IteratorWithResources<I> wrapped, Function<I, O> mapper) {
    this(wrapped, mapper, Collections.emptyList());
  }

  public MappingIteratorWithResources(
      IteratorWithResources<I> wrapped, Function<I, O> mapper, List<AutoCloseable> resources) {
    this.wrapped = wrapped;
    this.mapper = mapper;
    this.resources = unmodifiableList(resources);
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
    for (AutoCloseable resource : resources) {
      resource.close();
    }
    wrapped.close();
  }
}

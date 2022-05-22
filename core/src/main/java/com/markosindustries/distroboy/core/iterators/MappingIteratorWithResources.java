package com.markosindustries.distroboy.core.iterators;

import static java.util.Collections.unmodifiableList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * An {@link IteratorWithResources} which will take elements in the wrapped iterator and apply a map
 * operation to them. If the wrapped iterator is AutoCloseable (such as {@link
 * IteratorWithResources}) it will be closed appropriately too.
 *
 * @param <I> The type of elements in the input {@link Iterator}
 * @param <O> The type of elements in the output {@link IteratorWithResources}
 */
public class MappingIteratorWithResources<I, O> implements IteratorWithResources<O> {
  private final Iterator<I> wrapped;
  private final Function<I, O> mapper;
  private final List<AutoCloseable> resources;

  /**
   * Create a mapping iterator around an existing iterator. Note that the existing iterator must not
   * be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param mapper The function to map wrapped iterator items to the output type
   */
  public MappingIteratorWithResources(Iterator<I> wrapped, Function<I, O> mapper) {
    this(wrapped, mapper, Collections.emptyList());
  }

  /**
   * Create a mapping iterator around an existing iterator. Note that the existing iterator must not
   * be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param mapper The function to map wrapped iterator items to the output type
   * @param resources A set of resources which need to be closed when this {@link
   *     IteratorWithResources} is closed
   */
  public MappingIteratorWithResources(
      Iterator<I> wrapped, Function<I, O> mapper, List<? extends AutoCloseable> resources) {
    if (Objects.isNull(wrapped)) {
      throw new IllegalArgumentException("Wrapped iterator cannot be null");
    }
    if (Objects.isNull(mapper)) {
      throw new IllegalArgumentException("Mapper cannot be null");
    }
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
    if (wrapped instanceof AutoCloseable) {
      ((AutoCloseable) wrapped).close();
    }
  }
}

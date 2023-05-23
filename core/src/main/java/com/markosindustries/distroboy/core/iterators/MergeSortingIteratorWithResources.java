package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MergeSortingIteratorWithResources<I> extends MergeSortingIterator<I>
    implements IteratorWithResources<I> {
  private final List<AutoCloseable> closeables;

  public MergeSortingIteratorWithResources(
      List<? extends Iterator<I>> sortedIteratorsToMergeSort, Comparator<I> comparator) {
    this(sortedIteratorsToMergeSort, comparator, Collections.emptyList());
  }

  public MergeSortingIteratorWithResources(
      final List<? extends Iterator<I>> sortedIteratorsToMergeSort,
      final Comparator<I> comparator,
      final List<? extends AutoCloseable> resources) {
    super(sortedIteratorsToMergeSort, comparator);
    this.closeables =
        Stream.concat(
                resources.stream(),
                sortedIteratorsToMergeSort.stream()
                    .filter(x -> x instanceof AutoCloseable)
                    .map(x -> (AutoCloseable) x))
            .collect(Collectors.toList());
  }

  @Override
  public void close() throws Exception {
    for (final var closeable : closeables) {
      closeable.close();
    }
  }
}

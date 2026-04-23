package com.markosindustries.distroboy.core.iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

public class MergeSortingIterator<T> implements Iterator<T> {
  private final List<? extends Iterator<T>> sourceIterators;
  private final TreeMap<T, Integer> nextFromEachWithIteratorIndex;

  public MergeSortingIterator(
      List<? extends Iterator<T>> sortedIteratorsToMergeSort, Comparator<T> comparator) {
    this.sourceIterators = sortedIteratorsToMergeSort;
    this.nextFromEachWithIteratorIndex = new TreeMap<>(comparator);

    for (int i = 0; i < sourceIterators.size(); i++) {
      final var sourceIterator = sourceIterators.get(i);
      if (sourceIterator.hasNext()) {
        nextFromEachWithIteratorIndex.put(sourceIterator.next(), i);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !nextFromEachWithIteratorIndex.isEmpty();
  }

  @Override
  public T next() {
    final var next = nextFromEachWithIteratorIndex.firstEntry();
    nextFromEachWithIteratorIndex.remove(next.getKey());
    final var sourceIteratorIndex = next.getValue();
    final var sourceIterator = sourceIterators.get(sourceIteratorIndex);
    if (sourceIterator.hasNext()) {
      nextFromEachWithIteratorIndex.put(sourceIterator.next(), sourceIteratorIndex);
    }
    return next.getKey();
  }
}

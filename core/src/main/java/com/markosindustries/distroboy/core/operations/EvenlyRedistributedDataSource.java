package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.DataReferenceList;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class EvenlyRedistributedDataSource<I> implements DataSource<DataReferenceRange> {
  private final List<DataReference> dataReferences;

  public EvenlyRedistributedDataSource(DataReferenceList<I> dataReferences) {
    this.dataReferences = dataReferences.list();
    this.count = this.dataReferences.stream().mapToLong(DataReference::getCount).sum();
  }

  private final long count;

  @Override
  public long countOfFullSet() {
    return count;
  }

  @Override
  public IteratorWithResources<DataReferenceRange> enumerateRangeOfFullSet(
      final long startInclusive, final long endExclusive) {
    final Deque<DataReference> remoteDataReferencesToIterate = new ArrayDeque<>(dataReferences);
    if (remoteDataReferencesToIterate.isEmpty() || startInclusive == endExclusive) {
      return IteratorWithResources.emptyIterator();
    }

    long toSkip = startInclusive;
    while (toSkip > 0) {
      final var nextReferenceCount = remoteDataReferencesToIterate.peekFirst().getCount();

      if (toSkip < nextReferenceCount) {
        break;
      }

      remoteDataReferencesToIterate.removeFirst();
      toSkip -= nextReferenceCount;
    }

    final var ranges = new ArrayList<DataReferenceRange>();
    long toInclude = endExclusive - startInclusive;
    while (toInclude > 0) {
      final var nextReference = remoteDataReferencesToIterate.removeFirst();
      final var nextRange =
          DataSourceRange.newBuilder()
              .setStartInclusive(toSkip)
              .setEndExclusive(Math.min(toSkip + toInclude, nextReference.getCount()));
      ranges.add(
          DataReferenceRange.newBuilder().setReference(nextReference).setRange(nextRange).build());
      toInclude -= nextRange.getEndExclusive() - nextRange.getStartInclusive();
      toSkip = 0;
    }

    return IteratorWithResources.from(ranges.iterator());
  }
}

package distroboy.core.operations;

import distroboy.schemas.DataReference;
import distroboy.schemas.DataReferenceRange;
import distroboy.schemas.DataSourceRange;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class ClusterPersistedDataSource implements DataSource<DataReferenceRange> {
  private final List<DataReference> clusterMemberDataReferences;

  public ClusterPersistedDataSource(List<DataReference> clusterMemberDataReferences) {
    this.clusterMemberDataReferences = clusterMemberDataReferences;
    this.count = clusterMemberDataReferences.stream().mapToLong(DataReference::getCount).sum();
  }

  private final long count;

  @Override
  public long countOfFullSet() {
    return count;
  }

  @Override
  public Iterator<DataReferenceRange> enumerateRangeOfFullSet(
      final long startInclusive, final long endExclusive) {
    final Deque<DataReference> remoteDataReferencesToIterate =
        new ArrayDeque<>(clusterMemberDataReferences);
    if (remoteDataReferencesToIterate.isEmpty() || startInclusive == endExclusive) {
      return Collections.emptyIterator();
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

    return ranges.iterator();
  }
}

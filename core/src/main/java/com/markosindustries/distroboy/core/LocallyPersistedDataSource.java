package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.core.clustering.ClusterMember;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.DataSource;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.List;

class LocallyPersistedDataSource<I> implements DataSource<I> {
  private final ClusterMember clusterMember;
  private final long count;
  private final List<DataReference> dataReferencesList;

  LocallyPersistedDataSource(
      final ClusterMember clusterMember, final PersistedDataReferenceList<I> dataReferences) {
    this.clusterMember = clusterMember;
    this.dataReferencesList = dataReferences.list();
    this.count = dataReferencesList.stream().mapToLong(DataReference::getCount).sum();
  }

  @Override
  public long countOfFullSet() {
    return count;
  }

  @Override
  public IteratorWithResources<I> enumerateRangeOfFullSet(
      final long startInclusive, final long endExclusive) {
    // Ignore assigned range and just grab the data from this node
    return IteratorWithResources.from(
        clusterMember.retrieveLocalDataReferences(dataReferencesList));
  }
}

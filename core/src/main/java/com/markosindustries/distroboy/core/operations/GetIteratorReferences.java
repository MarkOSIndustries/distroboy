package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.DataReferenceList;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.PersistedDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;

class GetIteratorReferences<I> implements Operation<I, DataReference, DataReferenceList<I>> {
  private final Cluster cluster;
  private final Serialiser<I> serialiser;

  GetIteratorReferences(Cluster cluster, Serialiser<I> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    final DataReferenceId referenceId = new DataReferenceId();
    cluster.persistedData.store(referenceId, new PersistedDataReference<>(() -> input, serialiser));

    return IteratorWithResources.of(
        DataReference.newBuilder()
            .setMemberId(cluster.clusterMemberId.asBytes())
            .setReferenceId(referenceId.asBytes())
            .setUncountable(true)
            .build());
  }

  @Override
  public DataReferenceList<I> collect(Iterator<DataReference> results) {
    return new DataReferenceList<I>(IteratorTo.list(results));
  }
}

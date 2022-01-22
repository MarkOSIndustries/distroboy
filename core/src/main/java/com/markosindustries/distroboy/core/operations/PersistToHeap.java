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
import java.util.List;

public class PersistToHeap<I> implements Operation<I, DataReference, DataReferenceList<I>> {
  private final Cluster cluster;
  private final Serialiser<I> serialiser;

  public PersistToHeap(Cluster cluster, Serialiser<I> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    final List<I> asList = IteratorTo.list(input);

    final DataReferenceId referenceId = new DataReferenceId();
    cluster.persistedData.store(
        referenceId, new PersistedDataReference<>(asList::iterator, serialiser));

    return IteratorWithResources.from(
        List.of(
                DataReference.newBuilder()
                    .setMemberId(cluster.clusterMemberId.asBytes())
                    .setReferenceId(referenceId.asBytes())
                    .setCount(asList.size())
                    .build())
            .iterator());
  }

  @Override
  public DataReferenceList<I> collect(Iterator<DataReference> results) {
    return new DataReferenceList<I>(IteratorTo.list(results));
  }
}

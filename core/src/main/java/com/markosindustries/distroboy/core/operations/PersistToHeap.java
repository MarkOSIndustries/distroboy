package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.PersistedDataReferenceList;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.DistributableDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;

/**
 * Have each node in the cluster persist its fragment of the data to the heap. <b>WARNING:</b> if
 * the data doesn't fit in the available heap memory, the entire job will fail.
 *
 * @param <I> The type of the data being persisted
 */
public class PersistToHeap<I>
    implements Operation<I, DataReference, PersistedDataReferenceList<I>> {
  private final Cluster cluster;
  private final Serialiser<I> serialiser;

  /**
   * Have each node in the cluster persist its fragment of the data to the heap. <b>WARNING:</b> if
   * the data doesn't fit in the available heap memory, the entire job will fail.
   *
   * @param cluster The {@link Cluster} on which data is being persisted
   * @param serialiser A {@link Serialiser} for the data being persisted
   */
  public PersistToHeap(Cluster cluster, Serialiser<I> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    try (input) {
      final List<I> asList = IteratorTo.list(input);

      final DataReferenceId referenceId = new DataReferenceId();
      cluster.addDistributableData(
          referenceId,
          new DistributableDataReference<>(
              () -> IteratorWithResources.from(asList), serialiser, true));

      return IteratorWithResources.of(
          DataReference.newBuilder()
              .setMemberId(cluster.clusterMemberId.asBytes())
              .setReferenceId(referenceId.asBytes())
              .setCount(asList.size())
              .build());
    }
  }

  @Override
  public PersistedDataReferenceList<I> collect(Iterator<DataReference> results) {
    return new PersistedDataReferenceList<I>(IteratorTo.list(results));
  }
}

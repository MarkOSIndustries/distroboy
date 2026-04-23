package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.DataReferenceList;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.DistributableDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;

class GetIteratorReferences<Input>
    implements Operation<Input, DataReference, DataReferenceList<Input>> {
  private final Cluster cluster;
  private final Serialiser<Input> serialiser;

  GetIteratorReferences(Cluster cluster, Serialiser<Input> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<Input> input)
      throws Exception {
    final DataReferenceId referenceId = new DataReferenceId();
    cluster.addDistributableData(
        referenceId, new DistributableDataReference<>(() -> input, serialiser, false));

    return IteratorWithResources.of(
        DataReference.newBuilder()
            .setMemberId(cluster.clusterMemberId.asBytes())
            .setReferenceId(referenceId.asBytes())
            .setUncountable(true)
            .build());
  }

  @Override
  public DataReferenceList<Input> collect(Iterator<DataReference> results) {
    return new DataReferenceList<Input>(IteratorTo.list(results));
  }
}

package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.clustering.PersistedData;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class PersistToHeap<I> implements Operation<I, DataReference, List<DataReference>> {
  private final Serialiser<I> serialiser;

  public PersistToHeap(Serialiser<I> serialiser) {
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    final List<I> asList = IteratorTo.list(input);

    final UUID referenceId = UUID.randomUUID();

    PersistedData.STORED_REFERENCES.put(
        referenceId, new PersistedData<>(asList::iterator, serialiser));

    return IteratorWithResources.from(
        List.of(
                DataReference.newBuilder()
                    .setMemberId(ClusterMemberId.uuidAsBytes(ClusterMemberId.self))
                    .setReferenceId(ClusterMemberId.uuidAsBytes(referenceId))
                    .setCount(asList.size())
                    .build())
            .iterator());
  }

  @Override
  public List<DataReference> collect(Iterator<DataReference> results) {
    return IteratorTo.list(results);
  }
}

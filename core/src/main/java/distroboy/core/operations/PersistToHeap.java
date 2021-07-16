package distroboy.core.operations;

import static distroboy.core.clustering.ClusterMemberId.uuidAsBytes;

import distroboy.core.clustering.ClusterMemberId;
import distroboy.core.clustering.PersistedData;
import distroboy.core.clustering.serialisation.Serialiser;
import distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class PersistToHeap<I> implements Operation<I, DataReference, List<DataReference>> {
  private final Serialiser<I> serialiser;

  public PersistToHeap(Serialiser<I> serialiser) {
    this.serialiser = serialiser;
  }

  @Override
  public Iterator<DataReference> apply(Iterator<I> input) {
    final List<I> asList = ListFrom.iterator(input);

    final UUID referenceId = UUID.randomUUID();

    PersistedData.storedReferences.put(
        referenceId, () -> new MappingIterator<>(asList.iterator(), serialiser::serialise));

    return List.of(
            DataReference.newBuilder()
                .setMemberId(uuidAsBytes(ClusterMemberId.self))
                .setReferenceId(uuidAsBytes(referenceId))
                .setCount(asList.size())
                .build())
        .iterator();
  }

  @Override
  public List<DataReference> collect(Iterator<DataReference> results) {
    return ListFrom.iterator(results);
  }
}

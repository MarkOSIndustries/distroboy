package com.markosindustries.distroboy.core.operations;

import com.google.protobuf.CodedOutputStream;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.PersistedDataReferenceList;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.PersistedDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.protobuf.ProtobufFileIterator;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.Value;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Iterator;

/**
 * Have each node in the cluster persist its fragment of the data to disk. <b>WARNING:</b> if the
 * data doesn't fit in the available temp dir space, the entire job will fail.
 *
 * @param <I> The type of the data being persisted
 */
public class PersistToDisk<I>
    implements Operation<I, DataReference, PersistedDataReferenceList<I>> {
  private final Cluster cluster;
  private final Serialiser<I> serialiser;

  /**
   * Have each node in the cluster persist its fragment of the data to disk. <b>WARNING:</b> if the
   * data doesn't fit in the available temp dir space, the entire job will fail.
   *
   * @param cluster The {@link Cluster} on which data is being persisted
   * @param serialiser A {@link Serialiser} for the data being persisted
   */
  public PersistToDisk(Cluster cluster, Serialiser<I> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;
  }

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    final var file = File.createTempFile("db_persist_", ".bin");
    file.deleteOnExit();

    long valueCount = 0;
    try (final var fileOutputStream = new FileOutputStream(file, false)) {
      CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(fileOutputStream, 4096);

      while (input.hasNext()) {
        codedOutputStream.writeByteArrayNoTag(serialiser.serialise(input.next()).toByteArray());
        valueCount++;
      }
      codedOutputStream.flush();
    }

    final DataReferenceId referenceId = new DataReferenceId();
    cluster.persistedData.store(
        referenceId,
        new PersistedDataReference<>(
            () -> {
              try {
                return serialiser.deserialiseIteratorWithResources(
                    new ProtobufFileIterator<>(file, Value::parseFrom));
              } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
              }
            },
            serialiser));

    return IteratorWithResources.of(
        DataReference.newBuilder()
            .setMemberId(cluster.clusterMemberId.asBytes())
            .setReferenceId(referenceId.asBytes())
            .setCount(valueCount)
            .build());
  }

  @Override
  public PersistedDataReferenceList<I> collect(Iterator<DataReference> results) {
    return new PersistedDataReferenceList<I>(IteratorTo.list(results));
  }
}

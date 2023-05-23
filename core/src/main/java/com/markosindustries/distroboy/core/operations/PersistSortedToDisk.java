package com.markosindustries.distroboy.core.operations;

import com.google.protobuf.CodedOutputStream;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.SortedDataReferenceList;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.DistributableDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MergeSortingIteratorWithResources;
import com.markosindustries.distroboy.core.protobuf.ProtobufFileIterator;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.Value;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Have each node in the cluster sort and persist its fragment of the data to disk, such that items
 * will be read in the order specified by the given {@link Comparator}. <b>WARNING:</b> if the data
 * doesn't fit in the available temp dir space, the entire job will fail.
 *
 * @param <I> The type of the data being persisted
 */
public class PersistSortedToDisk<I>
    implements Operation<I, DataReference, SortedDataReferenceList<I>> {
  private final Cluster cluster;
  private final Serialiser<I> serialiser;
  private final Comparator<I> comparator;

  /**
   * Have each node in the cluster persist its fragment of the data to disk. <b>WARNING:</b> if the
   * data doesn't fit in the available temp dir space, the entire job will fail.
   *
   * @param cluster The {@link Cluster} on which data is being persisted
   * @param serialiser A {@link Serialiser} for the data being persisted
   * @param comparator A {@link Comparator} used to sort the data being persisted
   */
  public PersistSortedToDisk(Cluster cluster, Serialiser<I> serialiser, Comparator<I> comparator) {
    this.cluster = cluster;
    this.serialiser = serialiser;
    this.comparator = comparator;
  }

  // TODO - probably make this configurable?
  private static final int ITEMS_PER_FILE = 10_000;

  @Override
  public IteratorWithResources<DataReference> apply(IteratorWithResources<I> input)
      throws Exception {
    long valueCount = 0;

    final var protobufFileIteratorSuppliers = new ArrayList<Supplier<IteratorWithResources<I>>>();

    try (input) {
      while (input.hasNext()) {
        final var itemsForNextFile = new ArrayList<I>(ITEMS_PER_FILE);
        for (int i = 0; i < ITEMS_PER_FILE && input.hasNext(); i++) {
          itemsForNextFile.add(input.next());
        }
        itemsForNextFile.sort(comparator);

        final var file = File.createTempFile("db_persist_", ".bin");
        file.deleteOnExit();
        try (final var fileOutputStream = new FileOutputStream(file, false)) {
          CodedOutputStream codedOutputStream =
              CodedOutputStream.newInstance(fileOutputStream, 4096);
          for (final I item : itemsForNextFile) {
            codedOutputStream.writeByteArrayNoTag(serialiser.serialise(item).toByteArray());
          }
          valueCount += itemsForNextFile.size();
          codedOutputStream.flush();
        }
        protobufFileIteratorSuppliers.add(
            () -> {
              try {
                return serialiser.deserialiseIteratorWithResources(
                    new ProtobufFileIterator<>(file, Value::parseFrom));
              } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    final DataReferenceId referenceId = new DataReferenceId();
    cluster.addDistributableData(
        referenceId,
        new DistributableDataReference<>(
            () -> {
              return new MergeSortingIteratorWithResources<>(
                  protobufFileIteratorSuppliers.stream()
                      .map(Supplier::get)
                      .collect(Collectors.toList()),
                  comparator);
            },
            serialiser,
            true));

    return IteratorWithResources.of(
        DataReference.newBuilder()
            .setMemberId(cluster.clusterMemberId.asBytes())
            .setReferenceId(referenceId.asBytes())
            .setCount(valueCount)
            .setSorted(true)
            .build());
  }

  @Override
  public SortedDataReferenceList<I> collect(Iterator<DataReference> results) {
    return new SortedDataReferenceList<I>(IteratorTo.list(results));
  }
}

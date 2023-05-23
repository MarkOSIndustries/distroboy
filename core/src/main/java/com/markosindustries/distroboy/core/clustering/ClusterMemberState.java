package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferences;
import com.markosindustries.distroboy.schemas.SortRange;
import com.markosindustries.distroboy.schemas.Value;
import io.grpc.stub.StreamObserver;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

class ClusterMemberState {
  final BlockingQueue<Job> jobs = new LinkedBlockingQueue<>();
  final BlockingQueueMap<DataReferenceId, HashedDataReference<?>> dataReferenceHashers =
      new BlockingQueueMap<>();
  final BlockingQueue<SynchronisationPoint> synchronisationPoints = new LinkedBlockingQueue<>();
  final Object synchronisationPointsLock = new Object();
  final AtomicReference<DataReferences> distributedReferences = new AtomicReference<>(null);
  final Object distributedReferencesLock = new Object();
  final BlockingQueueMap<DataReferenceId, DataReferenceSortSamples> dataReferenceSortSamples =
      new BlockingQueueMap<>();
  final BlockingQueueMap<DataReferenceId, SortedDataReference<?>> dataReferenceComparators =
      new BlockingQueueMap<>();

  final DistributableData distributableData = new DistributableData();

  boolean isLeader;

  static class SynchronisationPoint {
    volatile Value synchronisedValue = null;
    final Supplier<Value> supplyValue;
    final CountDownLatch countDownLatch;

    public <T> SynchronisationPoint(
        final Supplier<T> valueSupplier,
        final Serialiser<T> valueSerialiser,
        final int expectedSynchroniseCount) {
      supplyValue = () -> valueSerialiser.serialise(valueSupplier.get());
      countDownLatch = new CountDownLatch(expectedSynchroniseCount);
    }

    public Value getSynchronisedValue() {
      if (synchronisedValue == null) {
        synchronized (this) {
          if (synchronisedValue == null) {
            synchronisedValue = supplyValue.get();
          }
        }
      }
      return synchronisedValue;
    }
  }

  record HashRetriever(Integer hash, StreamObserver<Value> retriever) {}

  static class HashedDataReference<T> implements LastOneInShutsTheDoor.Party<HashRetriever> {
    final DataReference dataReference;
    final Function<T, Integer> hasher;
    final int expectedRetrieveCount;
    final Object lock;
    final ConcurrentMap<Integer, StreamObserver<Value>> retrieversByHash;

    public HashedDataReference(
        DataReference dataReference, Function<T, Integer> hasher, int expectedRetrieveCount) {
      this.dataReference = dataReference;
      this.hasher = hasher;
      this.expectedRetrieveCount = expectedRetrieveCount;
      this.lock = new Object();
      this.retrieversByHash = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public int hash(Object o) {
      return hasher.apply((T) o);
    }

    @Override
    public boolean enter(final HashRetriever member) {
      retrieversByHash.put(member.hash(), member.retriever());
      return retrieversByHash.size() == expectedRetrieveCount;
    }
  }

  record DataReferenceSortSamples(DataReference dataReference, List<Value> samples) {}

  record SortRangeRetriever(SortRange sortRange, StreamObserver<Value> retriever) {}

  static class SortedDataReference<T> implements LastOneInShutsTheDoor.Party<SortRangeRetriever> {
    final DataReference dataReference;
    final Comparator<T> comparator;
    final Serialiser<T> serialiser;
    final int expectedRetrieveCount;
    final Object lock;
    final TreeMap<T, SortRangeRetriever> retrieversByRangeEndInclusive;

    public SortedDataReference(
        DataReference dataReference,
        Comparator<T> comparator,
        Serialiser<T> serialiser,
        int expectedRetrieveCount) {
      this.dataReference = dataReference;
      this.comparator = comparator;
      this.serialiser = serialiser;
      this.expectedRetrieveCount = expectedRetrieveCount;
      this.lock = new Object();
      this.retrieversByRangeEndInclusive = new TreeMap<>(comparator);
    }

    @Override
    public boolean enter(final SortRangeRetriever member) {
      try {
        retrieversByRangeEndInclusive.put(
            serialiser.deserialise(member.sortRange.getRangeEndInclusive()), member);
        return retrieversByRangeEndInclusive.size() == expectedRetrieveCount;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    public int compare(final Object o1, final Object o2) {
      return comparator.compare((T) o1, (T) o2);
    }
  }
}

package com.markosindustries.distroboy.core.clustering;

import static com.markosindustries.distroboy.core.DataSourceRanges.describeRange;

import com.google.protobuf.Empty;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.ClusterMemberGrpc;
import com.markosindustries.distroboy.schemas.ClusterMemberIdentity;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferenceSortRange;
import com.markosindustries.distroboy.schemas.DataReferences;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.Value;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMemberListener extends ClusterMemberGrpc.ClusterMemberImplBase {
  private static final Logger log = LoggerFactory.getLogger(ClusterMember.class);

  private final Cluster cluster;
  private final ClusterMemberState clusterMemberState;

  public ClusterMemberListener(Cluster cluster, ClusterMemberState clusterMemberState) {
    this.cluster = cluster;
    this.clusterMemberState = clusterMemberState;
  }

  @Override
  public void identify(Empty request, StreamObserver<ClusterMemberIdentity> responseObserver) {
    responseObserver.onNext(
        ClusterMemberIdentity.newBuilder().setNodeId(cluster.clusterMemberId.asBytes()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void process(DataSourceRange dataSourceRange, StreamObserver<Value> responseObserver) {
    try {
      log.debug("{} Starting job {}", cluster.clusterMemberId, describeRange(dataSourceRange));
      Job job = null;
      while (job == null) {
        try {
          job = clusterMemberState.jobs.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // Ignore, just try again
        }
      }

      log.debug("Responding to job {}", describeRange(dataSourceRange));
      try (IteratorWithResources<Value> iterator = job.execute(dataSourceRange)) {
        // TODO: backpressure
        while (iterator.hasNext()) {
          final var value = iterator.next();
          responseObserver.onNext(value);
        }
      }
      responseObserver.onCompleted();
      log.debug("Finished job {}", describeRange(dataSourceRange));
    } catch (Exception e) {
      log.error("Failed while executing job", e);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void distribute(DataReferences request, StreamObserver<Empty> responseObserver) {
    synchronized (clusterMemberState.distributedReferencesLock) {
      while (clusterMemberState.distributedReferences.get() != null) {
        try {
          clusterMemberState.distributedReferencesLock.wait();
        } catch (InterruptedException e) {
          // Ignore, keep trying...
        }
      }
      clusterMemberState.distributedReferences.set(request);
      clusterMemberState.distributedReferencesLock.notify();
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void retrieveRange(DataReferenceRange request, StreamObserver<Value> responseObserver) {
    final var memberId = ClusterMemberId.fromBytes(request.getReference().getMemberId());
    if (!cluster.clusterMemberId.equals(memberId)) {
      log.error(
          "Wrong memberId for data range retrieval, this is probably broken -- {} {}",
          memberId,
          cluster.clusterMemberId);
    }

    // TODO: backpressure
    final var distributableDataReference =
        clusterMemberState.distributableData.retrieve(
            DataReferenceId.fromBytes(request.getReference().getReferenceId()));
    try (final var iterator = distributableDataReference.getSerialisingIterator()) {
      long startInclusive = request.getRange().getStartInclusive();
      long endExclusive = request.getRange().getEndExclusive();
      long currentIndex = 0;
      while (iterator.hasNext()) {
        try {
          final var value = iterator.next();
          if (currentIndex < startInclusive) {
            continue;
          }
          if (currentIndex >= endExclusive) {
            break;
          }
          responseObserver.onNext(value);
        } finally {
          currentIndex++;
        }
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to close persisted data iterator - likely resource leakage", e);
      responseObserver.onError(Status.INTERNAL.asException());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void retrieveByHash(
      DataReferenceHashSpec request, StreamObserver<Value> responseObserver) {
    final var referenceId = DataReferenceId.fromBytes(request.getReference().getReferenceId());

    LastOneInShutsTheDoor.join(
            clusterMemberState.dataReferenceHashers,
            referenceId,
            new ClusterMemberState.HashRetriever(request.getHash(), responseObserver))
        .ifPresent(
            hashedDataReference -> {
              // TODO: backpressure
              final var distributableDataReference =
                  clusterMemberState.distributableData.retrieve(referenceId);

              try (final var iteratorWithSerialiser =
                  distributableDataReference.getIteratorWithSerialiser()) {
                while (iteratorWithSerialiser.hasNext()) {
                  final var next = iteratorWithSerialiser.next();
                  final var hash =
                      Math.abs(hashedDataReference.hash(next.value) % request.getModulo());
                  hashedDataReference.retrieversByHash.get(hash).onNext(next.serialise());
                }
                for (StreamObserver<Value> retriever :
                    hashedDataReference.retrieversByHash.values()) {
                  retriever.onCompleted();
                }
              } catch (Exception e) {
                log.warn("Failed to close persisted data iterator - likely resource leakage", e);
                responseObserver.onError(Status.INTERNAL.asException());
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public void retrieveSortSamples(
      final DataReference request, final StreamObserver<Value> responseObserver) {
    final var referenceId = DataReferenceId.fromBytes(request.getReferenceId());

    final var dataReferenceSortSamples =
        clusterMemberState.dataReferenceSortSamples.awaitPollValue(referenceId);

    for (final Value sample : dataReferenceSortSamples.samples()) {
      responseObserver.onNext(sample);
    }
    responseObserver.onCompleted();
  }

  @Override
  public void retrieveSortRange(
      final DataReferenceSortRange request, final StreamObserver<Value> responseObserver) {
    final var referenceId = DataReferenceId.fromBytes(request.getReference().getReferenceId());
    final var memberId = ClusterMemberId.fromBytes(request.getReference().getMemberId());
    if (!cluster.clusterMemberId.equals(memberId)) {
      log.error(
          "Wrong memberId for data range retrieval, this is probably broken -- {} {}",
          memberId,
          cluster.clusterMemberId);
    }

    LastOneInShutsTheDoor.join(
            clusterMemberState.dataReferenceComparators,
            referenceId,
            new ClusterMemberState.SortRangeRetriever(request.getSortRange(), responseObserver))
        .ifPresent(
            sortedDataReference -> {
              // TODO: backpressure
              final var distributableDataReference =
                  clusterMemberState.distributableData.retrieve(
                      DataReferenceId.fromBytes(request.getReference().getReferenceId()));

              final var orderedRangeRetrievers =
                  sortedDataReference.retrieversByRangeEndInclusive.entrySet().iterator();
              var currentRetriever = orderedRangeRetrievers.next();
              try (final var iterator = distributableDataReference.getIteratorWithSerialiser()) {
                while (iterator.hasNext()) {
                  final var valueWithSerialiser = iterator.next();
                  if (sortedDataReference.compare(
                          valueWithSerialiser.value, currentRetriever.getKey())
                      > 0) {
                    currentRetriever.getValue().retriever().onCompleted();
                    currentRetriever = orderedRangeRetrievers.next();
                  }

                  currentRetriever.getValue().retriever().onNext(valueWithSerialiser.serialise());
                }
                currentRetriever.getValue().retriever().onCompleted();
                while (orderedRangeRetrievers.hasNext()) {
                  orderedRangeRetrievers.next().getValue().retriever().onCompleted();
                }
              } catch (Exception e) {
                log.warn("Failed to close persisted data iterator - likely resource leakage", e);
                currentRetriever.getValue().retriever().onCompleted();
                while (orderedRangeRetrievers.hasNext()) {
                  orderedRangeRetrievers
                      .next()
                      .getValue()
                      .retriever()
                      .onError(Status.INTERNAL.asException());
                }
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public void synchronise(final Empty request, final StreamObserver<Value> responseObserver) {
    // Only the leader should materialise the value and synchronise it
    if (!clusterMemberState.isLeader) {
      responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
      responseObserver.onCompleted();
      return;
    }

    ClusterMemberState.SynchronisationPoint synchronisationPoint;
    synchronized (clusterMemberState.synchronisationPointsLock) {
      synchronisationPoint = clusterMemberState.synchronisationPoints.peek();
      while (synchronisationPoint == null) {
        try {
          clusterMemberState.synchronisationPointsLock.wait();
        } catch (InterruptedException e) {
          // Just try again
        }
        synchronisationPoint = clusterMemberState.synchronisationPoints.peek();
      }
    }

    synchronisationPoint.countDownLatch.countDown();

    while (true) {
      try {
        synchronisationPoint.countDownLatch.await();
        break;
      } catch (InterruptedException e) {
        // just try again
      }
    }

    // This one is now done, pop it off the queue
    clusterMemberState.synchronisationPoints.poll();

    responseObserver.onNext(synchronisationPoint.getSynchronisedValue());
    responseObserver.onCompleted();
  }
}

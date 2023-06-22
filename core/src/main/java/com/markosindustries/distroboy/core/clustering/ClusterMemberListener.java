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
import com.markosindustries.distroboy.schemas.SynchronisationPoint;
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
        job = clusterMemberState.jobs.poll(10, TimeUnit.SECONDS);
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
    } catch (Exception ex) {
      log.error("Failed while executing job", ex);
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void distribute(DataReferences request, StreamObserver<Empty> responseObserver) {
    try {
      synchronized (clusterMemberState.distributedReferencesLock) {
        while (clusterMemberState.distributedReferences.get() != null) {
          if (clusterMemberState.disbanding.get()) {
            throw new RuntimeException("Cluster needs to shut down, disbanding");
          }
          clusterMemberState.distributedReferencesLock.wait(100);
        }
        clusterMemberState.distributedReferences.set(request);
        clusterMemberState.distributedReferencesLock.notify();
      }
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      log.error("Failed while distributing references", ex);
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
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
    } catch (Exception ex) {
      log.warn("Failed to close persisted data iterator - likely resource leakage", ex);
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void retrieveByHash(
      DataReferenceHashSpec request, StreamObserver<Value> responseObserver) {
    try {
      final var referenceId = DataReferenceId.fromBytes(request.getReference().getReferenceId());

      LastOneInShutsTheDoor.join(
              clusterMemberState.dataReferenceHashers,
              referenceId,
              new ClusterMemberState.HashRetriever(request.getHash(), responseObserver),
              clusterMemberState.disbanding)
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
                } catch (Exception ex) {
                  log.warn("Failed to close persisted data iterator - likely resource leakage", ex);
                  clusterMemberState.disbanding.set(true);
                  for (StreamObserver<Value> retriever :
                      hashedDataReference.retrieversByHash.values()) {
                    try {
                      retriever.onError(Status.INTERNAL.asException());
                    } catch (Exception unused) {
                      // Ignore, we probably already called onCompleted on this one
                    }
                  }
                }
              });
    } catch (Exception ex) {
      log.warn("Failed to join retrieveByHash party");
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void retrieveSortSamples(
      final DataReference request, final StreamObserver<Value> responseObserver) {
    try {
      final var referenceId = DataReferenceId.fromBytes(request.getReferenceId());

      final var dataReferenceSortSamples =
          clusterMemberState.dataReferenceSortSamples.awaitPollValue(
              referenceId, clusterMemberState.disbanding);
      for (final Value sample : dataReferenceSortSamples.samples()) {
        responseObserver.onNext(sample);
      }
      responseObserver.onCompleted();
    } catch (Exception ex) {
      log.warn("Failed to sent data samples");
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void retrieveSortRange(
      final DataReferenceSortRange request, final StreamObserver<Value> responseObserver) {
    try {
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
              new ClusterMemberState.SortRangeRetriever(request.getSortRange(), responseObserver),
              clusterMemberState.disbanding)
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
                } catch (Exception ex) {
                  log.warn("Failed to close persisted data iterator - likely resource leakage", ex);
                  clusterMemberState.disbanding.set(true);
                  currentRetriever.getValue().retriever().onError(Status.INTERNAL.asException());
                  while (orderedRangeRetrievers.hasNext()) {
                    orderedRangeRetrievers
                        .next()
                        .getValue()
                        .retriever()
                        .onError(Status.INTERNAL.asException());
                  }
                }
              });
    } catch (Exception ex) {
      log.warn("Failed to join retrieveSortRange party");
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void synchronise(
      final SynchronisationPoint request, final StreamObserver<Value> responseObserver) {
    // Only the leader should materialise the value and synchronise it
    if (!clusterMemberState.isLeader) {
      responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
      return;
    }

    try {
      ClusterMemberState.SynchronisationPoint synchronisationPoint;
      synchronized (clusterMemberState.synchronisationPointsLock) {
        while ((synchronisationPoint = clusterMemberState.synchronisationPoints.peek()) == null) {
          if (clusterMemberState.disbanding.get()) {
            throw new RuntimeException("Cluster needs to shut down, disbanding");
          }
          clusterMemberState.synchronisationPointsLock.wait(100);
        }
      }

      log.debug("Sync request {} {}", synchronisationPoint.index, request.getIndex());
      if (synchronisationPoint.index != request.getIndex()) {
        clusterMemberState.disbanding.set(true);
      } else {
        synchronisationPoint.countDownLatch.countDown();
      }

      do {
        if (clusterMemberState.disbanding.get()) {
          throw new RuntimeException("Cluster needs to shut down, at least one member lost sync");
        }
      } while (!synchronisationPoint.countDownLatch.await(100, TimeUnit.MILLISECONDS));

      // This one is now done, pop it off the queue
      clusterMemberState.synchronisationPoints.poll();

      responseObserver.onNext(synchronisationPoint.getSynchronisedValue());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      log.error("Cluster needs to shut down", ex);
      clusterMemberState.disbanding.set(true);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void forceDisband(final Empty request, final StreamObserver<Empty> responseObserver) {
    clusterMemberState.disbanding.set(true);
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }
}

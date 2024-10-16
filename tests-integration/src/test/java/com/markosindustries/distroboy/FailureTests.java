package com.markosindustries.distroboy;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import ch.qos.logback.classic.Level;
import com.markosindustries.distroboy.core.Hashing;
import com.markosindustries.distroboy.core.Logging;
import com.markosindustries.distroboy.core.clustering.serialisation.IntegerValues;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.core.operations.StaticDataSource;
import com.markosindustries.distroboy.schemas.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FailureTests {
  @BeforeAll
  public static void configureLogging() {
    Logging.configureDefault().setLevel("com.markosindustries.distroboy", Level.DEBUG);
  }

  @Test
  public void terminatesClusterWhenNonLeaderNodesThrow() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4, 5);
    try {
      DistroBoySingleProcess.run(
          "FailureTests.terminatesClusterWhenNonLeaderNodesThrow",
          3,
          cluster -> {
            final var simpleJob =
                DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues)).count();
            cluster.executeAsync(simpleJob);

            if (!cluster.isLeader()) {
              throw new RuntimeException("I am dying on purpose");
            }

            // Use a synchronisation so that it messes with the synchronise in cluster shutdown
            cluster.waitForAllMembers();
          });
    } catch (Exception unused) {
      // Noop
    }
    // The goal is to make it here without hanging
  }

  @Test
  public void terminatesClusterWhenNonLeaderNodesThrowDuringMap() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4, 5);
    try {
      DistroBoySingleProcess.run(
          "FailureTests.terminatesClusterWhenNonLeaderNodesThrowDuringMap",
          3,
          cluster -> {
            final var simpleJob =
                DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                    .map(
                        x -> {
                          if (!cluster.isLeader()) {
                            throw new RuntimeException("I am dying on purpose");
                          } else {
                            return x;
                          }
                        })
                    .count();
            cluster.execute(simpleJob);

            // Use a synchronisation so that it messes with the synchronise in cluster shutdown
            cluster.waitForAllMembers();
          });
    } catch (Exception unused) {
      // Noop
    }
    // The goal is to make it here without hanging
  }

  @Test
  public void terminatesClusterWhenNonLeaderNodesThrowDuringGroupBy() throws Exception {
    try {
      final var modulo = 6;
      final var expectedValues = IntStream.range(1, 21).boxed().collect(toList());
      final var expectedMap = expectedValues.stream().collect(groupingBy(x -> x % modulo));

      DistroBoySingleProcess.run(
          "FailureTests.terminatesClusterWhenNonLeaderNodesThrowDuringGroupBy",
          3,
          cluster -> {
            final var groupedDataJob =
                DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                    .redistributeAndGroupBy(
                        cluster,
                        num -> num % modulo,
                        Hashing::integers,
                        modulo,
                        new IntegerValues() {
                          @Override
                          public Value serialise(final Integer value) {
                            if (cluster.isLeader()) {
                              throw new RuntimeException("I am dying on purpose");
                            }
                            return super.serialise(value);
                          }
                        })
                    .collect(
                        Serialisers.mapEntries(
                            Serialisers.integerValues,
                            Serialisers.listEntries(Serialisers.integerValues)));

            cluster
                .executeAsync(groupedDataJob)
                .onClusterLeader(
                    actualMap -> {
                      Assertions.assertIterableEquals(expectedMap.keySet(), actualMap.keySet());
                      Assertions.assertIterableEquals(
                          expectedMap.values().stream()
                              .map(x -> x.stream().sorted().toList())
                              .toList(),
                          actualMap.values().stream()
                              .map(x -> x.stream().sorted().toList())
                              .toList());
                    });

            if (!cluster.isLeader()) {
              throw new RuntimeException("I am dying on purpose");
            }
          });
    } catch (Exception unused) {
      // Noop
      var ignored = unused;
    }
    // The goal is to make it here without hanging
  }

  @Test
  public void terminatesClusterWhenNonLeaderNodesThrowDuringSort() throws Exception {
    try {
      final Comparator<Integer> comparator = Integer::compare;

      final var inputValues = List.of(11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6);
      final var expectedValues = new ArrayList<>(inputValues);
      expectedValues.sort(comparator);

      DistroBoySingleProcess.run(
          "FailureTests.terminatesClusterWhenNonLeaderNodesThrowDuringSort",
          3,
          cluster -> {
            final var simpleJob =
                DistributedOpSequence.readFrom(new StaticDataSource<>(inputValues))
                    .persistAndSortToDisk(cluster, Serialisers.integerValues, comparator);

            final var dataReferences = cluster.persistAndSort(simpleJob);

            final var redistributeJob =
                cluster
                    .redistributeOrderingBy(
                        dataReferences,
                        comparator,
                        new IntegerValues() {
                          @Override
                          public Value serialise(final Integer value) {
                            if (cluster.isLeader()) {
                              throw new RuntimeException("I am dying on purpose");
                            }
                            return super.serialise(value);
                          }
                        })
                    .collect(Serialisers.integerValues);

            cluster
                .execute(redistributeJob)
                .onClusterLeader(
                    sortedValues -> {
                      Assertions.assertEquals(expectedValues.size(), sortedValues.size());
                      for (int i = 0; i < expectedValues.size(); i++) {
                        Assertions.assertEquals(expectedValues.get(i), sortedValues.get(i));
                      }
                    });
          });
    } catch (Exception unused) {
      // Noop
    }
    // The goal is to make it here without hanging
  }

  @Test
  public void timeoutHonouredWhenOneClusterMemberDoesNotShowUpToTheLobby() throws Exception {
    assertThrows(
        Exception.class,
        () -> {
          DistroBoySingleProcess.run(
              "FailureTests.timeoutHonouredWhenOneClusterMemberDoesNotShowUpToTheLobby",
              2,
              3,
              cluster -> {
                fail("Cluster started with fewer members than it expected");
                // We shouldn't get here, we're expecting to fail at lobby join
              });
        });
  }
}

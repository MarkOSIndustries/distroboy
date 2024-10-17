package com.markosindustries.distroboy.example;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Hashing;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.example.filesystem.DirSource;
import com.markosindustries.distroboy.example.filesystem.ReadLinesFromFiles;
import com.markosindustries.distroboy.logback.Logging;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Logging.configureDefault();

    final var config = ConfigFactory.create(ExampleConfig.class);

    try (final var cluster =
        Cluster.newBuilder("distroboy-example", config.expectedMembers())
            .coordinator(config.coordinatorHost(), config.coordinatorPort())
            .memberPort(config.memberPort())
            .join()) {

      // Filtering and counting the lines in some files
      cluster
          .execute(
              DistributedOpSequence.readFrom(new DirSource("/sample-data/txt"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .count())
          .onClusterLeader(
              count -> {
                log.info("Lines beginning with 'yay': {}", count);
              });

      // Filtering and collecting the lines in some files
      cluster
          .execute(
              DistributedOpSequence.readFrom(new DirSource("/sample-data/txt"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .collect(Serialisers.stringValues))
          .onClusterLeader(
              lines -> {
                for (String l : lines) {
                  log.info("-- {}", l);
                }
              });

      // Persisting a set of results for re-use in later operations
      final var heapPersistedLines =
          cluster.persist(
              DistributedOpSequence.readFrom(new DirSource("/sample-data/txt"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .persistToHeap(cluster, Serialisers.stringValues));

      // Using persisted results in a new distributed operation - distributing evenly across the
      // cluster
      cluster
          .execute(
              cluster.redistributeEqually(heapPersistedLines, Serialisers.stringValues).count())
          .onClusterLeader(
              count -> log.info("Lines beginning with 'yay' via heap persistence: {}", count));

      // Using persisted results in a groupBy
      cluster
          .execute(
              cluster
                  .redistributeAndGroupBy(
                      heapPersistedLines,
                      line -> line.length(),
                      Hashing::integers,
                      10,
                      Serialisers.stringValues)
                  .mapValues(lines -> lines.size())
                  .collect(
                      Serialisers.mapEntries(Serialisers.integerValues, Serialisers.integerValues)))
          .onClusterLeader(
              map -> {
                map.forEach(
                    (length, lineCount) -> {
                      log.info(" -- {} lines starting with yay had length {}", lineCount, length);
                    });
              });

      if (config.runExampleS3Client()) {
        ExampleS3Client.runExampleS3Client(cluster, config, heapPersistedLines);
      }

      if (config.runExampleKafkaConsumer()) {
        ExampleKafkaConsumer.runKafkaExample(cluster);
      }
    }
  }
}

package distroboy.example;

import distroboy.core.Cluster;
import distroboy.core.Hashing;
import distroboy.core.Logging;
import distroboy.core.clustering.serialisation.Serialisers;
import distroboy.core.filesystem.DirSource;
import distroboy.core.filesystem.ReadLinesFromFiles;
import distroboy.core.operations.DistributedOpSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Logging.configureDefault();

    final var port = args.length > 0 ? Integer.parseInt(args[0]) : 7071;
    final var expectedMembers = args.length > 1 ? Integer.parseInt(args[1]) : 2;

    try (final var cluster =
        Cluster.newBuilder("distroboy.example", expectedMembers).memberPort(port).join()) {

      // Filtering and counting the lines in some files
      cluster
          .collect(
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .count())
          .onClusterLeader(
              count -> {
                log.info("Lines beginning with 'yay': {}", count);
              });

      // Filtering and collecting the lines in some files
      cluster
          .collect(
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy"))
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
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .persistToHeap(Serialisers.stringValues));

      // Using persisted results in a new distributed operation - distributing evenly across the
      // cluster
      cluster
          .collect(
              cluster.redistributeEqually(heapPersistedLines, Serialisers.stringValues).count())
          .onClusterLeader(
              count -> log.info("Lines beginning with 'yay' via heap persistence: {}", count));

      // Using persisted results in a groupBy
      cluster
          .collect(
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
    }
  }
}

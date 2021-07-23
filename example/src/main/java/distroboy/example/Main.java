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
          .ifGotResult(
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
          .ifGotResult(
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
          .ifGotResult(
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
                  .collect(
                      Serialisers.mapEntries(
                          Serialisers.integerValues,
                          Serialisers.listEntries(Serialisers.stringValues))))
          .ifGotResult(
              map -> {
                map.forEach(
                    (length, lines) -> {
                      log.info(
                          " -- {} lines starting with yay had length {}", lines.size(), length);
                    });
              });
    }
  }
}

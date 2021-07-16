package distroboy.example;

import distroboy.core.Cluster;
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

      cluster
          .runCollect(
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .count())
          .ifGotResult(
              count -> {
                log.info("Lines beginning with 'yay': {}", count);
              });

      cluster
          .runCollect(
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

      final var heapPersistedLines =
          cluster.runPersist(
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .persistToHeap(Serialisers.stringValues));

      cluster
          .runCollect(
              DistributedOpSequence.readFrom(heapPersistedLines)
                  .flatMap(
                      dataReference -> cluster.retrieve(dataReference, Serialisers.stringValues))
                  .count())
          .ifGotResult(
              count -> log.info("Lines beginning with 'yay' via heap persistence: {}", count));
    }
  }
}

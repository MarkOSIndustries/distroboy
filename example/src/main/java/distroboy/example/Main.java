package distroboy.example;

import distroboy.core.Cluster;
import distroboy.core.Hashing;
import distroboy.core.Logging;
import distroboy.core.clustering.ClusterMemberId;
import distroboy.core.clustering.serialisation.Serialisers;
import distroboy.core.filesystem.DirSource;
import distroboy.core.filesystem.ReadLinesFromFiles;
import distroboy.core.operations.DistributedOpSequence;
import distroboy.parquet.WriteViaAvroToParquetFiles;
import distroboy.parquet.WriteViaProtobufToParquetFiles;
import distroboy.schemas.HostAndPort;
import java.nio.file.Path;
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
          .execute(
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy/txt"))
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
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy/txt"))
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
              DistributedOpSequence.readFrom(new DirSource("/tmp/distroboy/txt"))
                  .flatMap(new ReadLinesFromFiles())
                  .filter(line -> line.startsWith("yay"))
                  .persistToHeap(Serialisers.stringValues));

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

      // Writing results as avro/parquet somewhere
      cluster
          .execute(
              cluster
                  .redistributeAndGroupBy(
                      heapPersistedLines,
                      line -> line.length(),
                      Hashing::integers,
                      10,
                      Serialisers.stringValues)
                  .map(
                      lineLengthWithLines ->
                          new SampleParquetOutputRecord(
                              new SampleParquetOutputRecord.InnerThing(
                                  lineLengthWithLines.getValue().get(0)),
                              lineLengthWithLines.getKey()))
                  .reduce(
                      new WriteViaAvroToParquetFiles<SampleParquetOutputRecord>(
                          Path.of(
                              "/tmp/distroboy/output.avro." + ClusterMemberId.self + ".parquet"),
                          SampleParquetOutputRecord.class))
                  .map(Object::toString)
                  .collect(Serialisers.stringValues))
          .onClusterLeader(
              outputFilePath -> {
                log.info("We wrote avro/parquet out to {}", outputFilePath);
              });

      // Writing results as protobuf/parquet somewhere
      cluster
          .execute(
              cluster
                  .redistributeAndGroupBy(
                      heapPersistedLines,
                      line -> line.length(),
                      Hashing::integers,
                      10,
                      Serialisers.stringValues)
                  .map(
                      lineLengthWithLines ->
                          HostAndPort.newBuilder()
                              .setHost(lineLengthWithLines.getValue().get(0))
                              .setPort(lineLengthWithLines.getKey())
                              .build())
                  .reduce(
                      new WriteViaProtobufToParquetFiles<HostAndPort>(
                          Path.of(
                              "/tmp/distroboy/output.protobuf."
                                  + ClusterMemberId.self
                                  + ".parquet"),
                          HostAndPort.class))
                  .map(Object::toString)
                  .collect(Serialisers.stringValues))
          .onClusterLeader(
              outputFilePath -> {
                log.info("We wrote protobuf/parquet out to {}", outputFilePath);
              });
    }
  }
}

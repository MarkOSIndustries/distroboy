package com.markosindustries.distroboy.example;

import static com.markosindustries.distroboy.parquet.ParquetProtobuf.toParquetProtobufBytes;

import com.google.common.base.Strings;
import com.markosindustries.distroboy.aws.s3.DownloadFromS3ToDisk;
import com.markosindustries.distroboy.aws.s3.S3KeysSource;
import com.markosindustries.distroboy.aws.s3.S3ObjectsSource;
import com.markosindustries.distroboy.aws.s3.parquet.S3ObjectInputFile;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Hashing;
import com.markosindustries.distroboy.core.PersistedDataReferenceList;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.example.avro.SampleParquetOutputRecord;
import com.markosindustries.distroboy.example.localstack.TempSdkHttpClientTrailingSlashAppender;
import com.markosindustries.distroboy.example.schemas.StringWithNumber;
import com.markosindustries.distroboy.parquet.ParquetAvroFilesWriterStrategy;
import com.markosindustries.distroboy.parquet.ReadViaAvroFromParquetFiles;
import com.markosindustries.distroboy.parquet.ReadViaProtobufFromParquet;
import com.markosindustries.distroboy.parquet.WriteToParquet;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public interface ExampleS3Client {
  Logger log = LoggerFactory.getLogger(ExampleS3Client.class);

  static void runExampleS3Client(
      Cluster cluster, ExampleConfig config, PersistedDataReferenceList<String> heapPersistedLines)
      throws URISyntaxException {
    final var s3ClientBuilder = S3Client.builder();
    if (!Strings.isNullOrEmpty(config.s3EndpointOverride())) {
      s3ClientBuilder.endpointOverride(new URI(config.s3EndpointOverride()));
    }
    if (config.useLocalStackForS3()) {
      s3ClientBuilder
          .httpClient(
              new TempSdkHttpClientTrailingSlashAppender(ApacheHttpClient.builder().build()))
          .credentialsProvider(AnonymousCredentialsProvider.create());
    }
    final var s3Client = s3ClientBuilder.region(Region.of(config.awsRegion())).build();

    // Writing results as avro/parquet to disk and S3
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
                    new WriteToParquet<SampleParquetOutputRecord, Path>(
                        new ParquetAvroFilesWriterStrategy<>(
                            (_ignored) -> {
                              return Path.of(
                                  "/output-data/output.avro."
                                      + cluster.clusterMemberId
                                      + ".parquet");
                            },
                            SampleParquetOutputRecord.class)))
                .flatMap(IteratorWithResources::from)
                .map(
                    path -> {
                      s3Client.putObject(
                          req -> req.bucket("distroboy-bucket").key("avro/" + path.getFileName()),
                          path);
                      return path;
                    })
                .map(Object::toString)
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            outputFilePaths -> {
              log.info("We wrote avro/parquet out to {}", outputFilePaths);
            });

    // Read the parquet/avro records back from S3 and count them
    cluster
        .execute(
            DistributedOpSequence.readFrom(
                    new S3KeysSource(cluster, s3Client, "distroboy-bucket", "avro"))
                .mapWithResources(new DownloadFromS3ToDisk(s3Client, "distroboy-bucket"))
                .flatMap(new ReadViaAvroFromParquetFiles<>(SampleParquetOutputRecord.class))
                .map(record -> record.innerThing.thingId + " " + record.someNumber)
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            records -> {
              for (String record : records) {
                log.info("We read back the avro records from S3: {}", record);
              }
            });

    // Writing results as protobuf/parquet S3 directly with logical file groupings
    cluster
        .execute(
            cluster
                .redistributeAndGroupBy(
                    heapPersistedLines,
                    line -> line.length(),
                    Hashing::integers,
                    10,
                    Serialisers.stringValues)
                .mapValues(
                    lines -> {
                      return lines.stream()
                          .map(
                              line ->
                                  StringWithNumber.newBuilder()
                                      .setSomeString(line)
                                      .setSomeNumber(55)
                                      .build())
                          .collect(toParquetProtobufBytes(StringWithNumber.class));
                    })
                .map(
                    (lineLength, linesParquet) -> {
                      final var s3Key = "protobuf/length=" + lineLength + ".parquet";
                      s3Client.putObject(
                          req -> req.bucket("distroboy-bucket").key(s3Key),
                          RequestBody.fromBytes(linesParquet));
                      return s3Key;
                    })
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            s3Keys -> {
              log.info("We uploaded protobuf/parquet to {}", s3Keys);
            });

    // Read the parquet/protobuf records back from S3 using range requests, which
    // are more efficient when only requesting some columns from a parquet file
    cluster
        .execute(
            DistributedOpSequence.readFrom(
                    new S3ObjectsSource(cluster, s3Client, "distroboy-bucket", "protobuf"))
                .map(
                    s3Object -> {
                      return new S3ObjectInputFile(s3Client, "distroboy-bucket", s3Object);
                    })
                .flatMap(new ReadViaProtobufFromParquet<S3ObjectInputFile, StringWithNumber>())
                .collect(Serialisers.protobufValues(StringWithNumber::parseFrom)))
        .onClusterLeader(
            protobufs -> {
              for (StringWithNumber protobuf : protobufs) {
                log.info(
                    "We read back the protobufs from S3: {} {}",
                    protobuf.getSomeString(),
                    protobuf.getSomeNumber());
              }
            });
  }
}

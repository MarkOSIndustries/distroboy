package distroboy.example;

import com.google.common.base.Strings;
import distroboy.aws.s3.DownloadFromS3ToDisk;
import distroboy.aws.s3.S3KeysSource;
import distroboy.aws.s3.UploadFromDiskToS3;
import distroboy.core.Cluster;
import distroboy.core.Hashing;
import distroboy.core.clustering.ClusterMemberId;
import distroboy.core.clustering.serialisation.Serialisers;
import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.DistributedOpSequence;
import distroboy.example.avro.SampleParquetOutputRecord;
import distroboy.example.localstack.TempSdkHttpClientTrailingSlashAppender;
import distroboy.example.schemas.StringWithNumber;
import distroboy.parquet.ReadViaAvroFromParquetFiles;
import distroboy.parquet.ReadViaProtobufFromParquetFiles;
import distroboy.parquet.WriteViaAvroToParquetFiles;
import distroboy.parquet.WriteViaProtobufToParquetFiles;
import distroboy.schemas.DataReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public interface ExampleS3Client {
  Logger log = LoggerFactory.getLogger(ExampleS3Client.class);

  static void runExampleS3Client(
      Cluster cluster, ExampleConfig config, List<DataReference> heapPersistedLines)
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
                    new WriteViaAvroToParquetFiles<SampleParquetOutputRecord>(
                        Path.of("/output-data/output.avro." + ClusterMemberId.self + ".parquet"),
                        SampleParquetOutputRecord.class))
                .flatMap(IteratorWithResources::from)
                .map(
                    new UploadFromDiskToS3<>(
                        s3Client,
                        "distroboy-bucket",
                        path -> "avro/" + path.getFileName(),
                        path -> path))
                .map(Object::toString)
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            outputFilePath -> {
              log.info("We wrote avro/parquet out to {}", outputFilePath);
            });

    // Writing results as protobuf/parquet to disk and S3
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
                        StringWithNumber.newBuilder()
                            .setSomeString(lineLengthWithLines.getValue().get(0))
                            .setSomeNumber(lineLengthWithLines.getKey())
                            .build())
                .reduce(
                    new WriteViaProtobufToParquetFiles<StringWithNumber>(
                        Path.of(
                            "/output-data/output.protobuf." + ClusterMemberId.self + ".parquet"),
                        StringWithNumber.class))
                .flatMap(IteratorWithResources::from)
                .map(
                    new UploadFromDiskToS3<>(
                        s3Client,
                        "distroboy-bucket",
                        path -> "protobuf/" + path.getFileName(),
                        path -> path))
                .map(Object::toString)
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            outputFilePath -> {
              log.info("We wrote protobuf/parquet out to {}", outputFilePath);
            });

    // Read the parquet/protobuf records back from S3 and count them
    cluster
        .execute(
            DistributedOpSequence.readFrom(
                    new S3KeysSource(s3Client, "distroboy-bucket", "protobuf"))
                .mapWithResources(new DownloadFromS3ToDisk(s3Client, "distroboy-bucket"))
                .flatMap(new ReadViaProtobufFromParquetFiles<StringWithNumber>())
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

    // Read the parquet/avro records back from S3 and count them
    cluster
        .execute(
            DistributedOpSequence.readFrom(new S3KeysSource(s3Client, "distroboy-bucket", "avro"))
                .mapWithResources(new DownloadFromS3ToDisk(s3Client, "distroboy-bucket"))
                .flatMap(
                    new ReadViaAvroFromParquetFiles<SampleParquetOutputRecord>(
                        SampleParquetOutputRecord.class))
                .map(record -> record.innerThing.thingId + " " + record.someNumber)
                .collect(Serialisers.stringValues))
        .onClusterLeader(
            records -> {
              for (String record : records) {
                log.info("We read back the avro records from S3: {}", record);
              }
            });
  }
}

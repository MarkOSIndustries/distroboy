package distroboy.aws.s3;

import distroboy.core.filesystem.TempDir;
import distroboy.core.operations.MapOp;
import java.nio.file.Path;
import java.util.UUID;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class DownloadFromS3ToDisk implements MapOp<String, Path> {
  private final S3Client s3Client;
  private final String bucket;

  public DownloadFromS3ToDisk(String region, String bucket) {
    this.s3Client = S3Client.builder().region(Region.of(region)).build();
    this.bucket = bucket;
  }

  @Override
  public Path map(String input) {
    final var last8Chars = input.substring(input.length() - Math.min(8, input.length()));
    final var path = TempDir.tempFile(UUID.randomUUID() + "_" + last8Chars);
    s3Client.getObject(req -> req.bucket(bucket).key(input), path);
    return path;
  }
}

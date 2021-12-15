package distroboy.aws.s3;

import distroboy.core.filesystem.TempDir;
import distroboy.core.operations.MapOp;
import java.io.File;
import java.io.IOException;
import software.amazon.awssdk.services.s3.S3Client;

public class DownloadFromS3ToHeap implements MapOp<String, byte[]> {
  private final S3Client s3Client;
  private final String bucket;

  public DownloadFromS3ToHeap(S3Client s3Client, String bucket) {
    this.s3Client = s3Client;
    this.bucket = bucket;
  }

  @Override
  public byte[] map(String input) {
    try {
      final var path = TempDir.tempFile(input.replace(File.separator, "_"));
      return s3Client.getObject(req -> req.bucket(bucket).key(input)).readAllBytes();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

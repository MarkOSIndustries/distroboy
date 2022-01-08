package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.ResultWithResource;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.operations.MapOpWithResources;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class DownloadFromS3ToDisk implements MapOpWithResources<String, Path> {
  private final S3Client s3Client;
  private final String bucket;

  public DownloadFromS3ToDisk(final S3Client s3Client, final String bucket) {
    this.s3Client = s3Client;
    this.bucket = bucket;
  }

  @Override
  public ResultWithResource<Path> map(String input) {
    try (final var readableByteChannel =
        Channels.newChannel(s3Client.getObject(req -> req.bucket(bucket).key(input)))) {
      final var file = File.createTempFile("db_s3_" + ClusterMemberId.self, ".bin");
      try (final var fileOutputStream = new FileOutputStream(file)) {
        fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
      }
      return new ResultWithResource<>(file.toPath(), file::delete);
    } catch (NoSuchKeyException | IOException noSuchKeyException) {
      throw new RuntimeException(noSuchKeyException);
    }
  }
}

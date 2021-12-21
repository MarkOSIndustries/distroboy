package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.operations.MapOp;
import java.nio.file.Path;
import java.util.function.Function;
import software.amazon.awssdk.services.s3.S3Client;

public class UploadFromDiskToS3<I> implements MapOp<I, I> {
  private final S3Client s3Client;
  private final String bucket;
  private final Function<I, String> keyAccessor;
  private final Function<I, Path> pathAccessor;

  public UploadFromDiskToS3(
      S3Client s3Client,
      String bucket,
      Function<I, String> keyAccessor,
      Function<I, Path> pathAccessor) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyAccessor = keyAccessor;
    this.pathAccessor = pathAccessor;
  }

  @Override
  public I map(I input) {
    String key = keyAccessor.apply(input);
    Path path = pathAccessor.apply(input);
    s3Client.putObject(req -> req.bucket(bucket).key(key), path);
    return input;
  }
}

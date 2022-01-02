package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.operations.ForEachOp;
import java.util.function.Function;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

public class StreamToS3<I> implements ForEachOp<I> {
  private final S3Client s3Client;
  private final String bucket;
  private final Function<I, String> keyAccessor;
  private final Function<I, RequestBody> requestBodyAccessor;

  public StreamToS3(
      S3Client s3Client,
      String bucket,
      Function<I, String> keyAccessor,
      Function<I, RequestBody> requestBodyAccessor) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyAccessor = keyAccessor;
    this.requestBodyAccessor = requestBodyAccessor;
  }

  @Override
  public void forEach(I input) {
    String key = keyAccessor.apply(input);
    RequestBody requestBody = requestBodyAccessor.apply(input);
    s3Client.putObject(req -> req.bucket(bucket).key(key), requestBody);
  }
}

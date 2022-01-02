package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.operations.ForEachOp;
import java.util.function.Function;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

public class UploadFromHeapToS3<I> implements ForEachOp<I> {
  private final S3Client s3Client;
  private final String bucket;
  private final Function<I, String> keyAccessor;
  private final Function<I, byte[]> bytesAccessor;

  public UploadFromHeapToS3(
      S3Client s3Client,
      String bucket,
      Function<I, String> keyAccessor,
      Function<I, byte[]> bytesAccessor) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyAccessor = keyAccessor;
    this.bytesAccessor = bytesAccessor;
  }

  @Override
  public void forEach(I input) {
    String key = keyAccessor.apply(input);
    byte[] bytes = bytesAccessor.apply(input);
    s3Client.putObject(req -> req.bucket(bucket).key(key), RequestBody.fromBytes(bytes));
  }
}

package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.operations.ForEachOp;
import java.util.function.Function;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

public class UploadFromHeapToS3<Input> implements ForEachOp<Input> {
  private final S3Client s3Client;
  private final String bucket;
  private final Function<Input, String> keyAccessor;
  private final Function<Input, byte[]> bytesAccessor;

  public UploadFromHeapToS3(
      S3Client s3Client,
      String bucket,
      Function<Input, String> keyAccessor,
      Function<Input, byte[]> bytesAccessor) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyAccessor = keyAccessor;
    this.bytesAccessor = bytesAccessor;
  }

  @Override
  public void forEach(Input input) {
    String key = keyAccessor.apply(input);
    byte[] bytes = bytesAccessor.apply(input);
    s3Client.putObject(req -> req.bucket(bucket).key(key), RequestBody.fromBytes(bytes));
  }
}

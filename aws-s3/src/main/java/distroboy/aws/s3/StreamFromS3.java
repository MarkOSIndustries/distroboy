package distroboy.aws.s3;

import distroboy.core.operations.MapOp;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class StreamFromS3 implements MapOp<String, ResponseInputStream<GetObjectResponse>> {
  private final S3Client s3Client;
  private final String bucket;

  public StreamFromS3(S3Client s3Client, String bucket) {
    this.s3Client = s3Client;
    this.bucket = bucket;
  }

  @Override
  public ResponseInputStream<GetObjectResponse> map(String input) {
    return s3Client.getObject(req -> req.bucket(bucket).key(input));
  }
}

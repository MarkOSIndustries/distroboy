package distroboy.aws.s3.parquet;

import distroboy.parquet.RangeRequestingSeekableInputStream;
import java.io.InputStream;
import software.amazon.awssdk.services.s3.S3Client;

public class S3RangeRetriever implements RangeRequestingSeekableInputStream.RangeRetriever {
  private final S3Client s3Client;
  private final String bucket;
  private final String key;

  public S3RangeRetriever(S3Client s3Client, String bucket, String key) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.key = key;
  }

  @Override
  public InputStream retrieve(long position, long bytes) {
    if (bytes <= 0) {
      return InputStream.nullInputStream();
    }
    // S3 range requests are start/end inclusive
    return s3Client.getObject(
        req ->
            req.bucket(bucket).key(key).range("bytes=" + position + "-" + (position + bytes - 1)));
  }
}

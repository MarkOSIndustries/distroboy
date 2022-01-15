package com.markosindustries.distroboy.aws.s3.parquet;

import com.markosindustries.distroboy.parquet.RangeRequestingSeekableInputStream;
import java.io.InputStream;
import software.amazon.awssdk.services.s3.S3Client;

/** A range retriever which can issue byte range requests to S3 */
public class S3RangeRetriever implements RangeRequestingSeekableInputStream.RangeRetriever {
  private final S3Client s3Client;
  private final String bucket;
  private final String key;

  /**
   * Create an S3RangeRetriever
   *
   * @param s3Client An S3Client
   * @param bucket The name of the bucket containing the file
   * @param key The key to access in the bucket
   */
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

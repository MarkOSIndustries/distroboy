package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.DataSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3KeysSource implements DataSource<String> {
  private final S3Client s3Client;
  private final String bucket;
  private final String keyPrefix;

  public S3KeysSource(final S3Client s3Client, final String bucket, final String keyPrefix) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyPrefix = keyPrefix;
  }

  @Override
  public long countOfFullSet() {
    ListObjectsV2Iterable responses =
        s3Client.listObjectsV2Paginator(req -> req.bucket(bucket).prefix(keyPrefix));
    return responses.stream().count();
  }

  @Override
  public IteratorWithResources<String> enumerateRangeOfFullSet(
      long startInclusive, long endExclusive) {
    ListObjectsV2Iterable responses =
        s3Client.listObjectsV2Paginator(req -> req.bucket(bucket).prefix(keyPrefix));
    return IteratorWithResources.from(
        responses.stream()
            .skip(startInclusive)
            .limit(endExclusive - startInclusive)
            .flatMap(response -> response.contents().stream().map(S3Object::key))
            .iterator());
  }
}

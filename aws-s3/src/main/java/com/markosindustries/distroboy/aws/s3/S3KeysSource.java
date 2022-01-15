package com.markosindustries.distroboy.aws.s3;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.InterleavedDataSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3KeysSource extends InterleavedDataSource<String> {
  private final S3Client s3Client;
  private final String bucket;
  private final String keyPrefix;

  public S3KeysSource(
      final Cluster cluster, final S3Client s3Client, final String bucket, final String keyPrefix) {
    super(cluster);
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.keyPrefix = keyPrefix;
  }

  @Override
  public IteratorWithResources<String> enumerateFullSet() {
    ListObjectsV2Iterable responses =
        s3Client.listObjectsV2Paginator(req -> req.bucket(bucket).prefix(keyPrefix));
    return IteratorWithResources.from(
        responses.stream()
            .flatMap(response -> response.contents().stream().map(S3Object::key))
            .iterator());
  }
}

package distroboy.aws.s3.parquet;

import distroboy.parquet.RangeRequestingSeekableInputStream;
import java.io.EOFException;
import java.io.IOException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3ObjectInputFile implements InputFile {
  private final S3Client s3Client;
  private final String bucket;
  private final Long size;
  private final String key;

  private static final int FOOTER_AND_MAGIC_BYTES = 8;

  public S3ObjectInputFile(S3Client s3Client, String bucket, S3Object s3Object) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.size = s3Object.size();
    this.key = s3Object.key();
  }

  @Override
  public long getLength() throws IOException {
    return size;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    final var rangeRetriever = new S3RangeRetriever(s3Client, bucket, key);

    // Prefetch the footer and magic bytes, since we know we'll need them
    // and the parquet code will try to read it via 5 requests rather than
    // one
    final var footerStartPos = size - FOOTER_AND_MAGIC_BYTES;
    try (final var stream = rangeRetriever.retrieve(footerStartPos, FOOTER_AND_MAGIC_BYTES)) {
      final var footerAndMagic = new byte[FOOTER_AND_MAGIC_BYTES];
      if (stream.readNBytes(footerAndMagic, 0, FOOTER_AND_MAGIC_BYTES) < 0) {
        throw new EOFException("Prefetched footer was too short");
      }
      return new PrefetchedFooterSeekableInputStream(
          new RangeRequestingSeekableInputStream(rangeRetriever, size),
          footerAndMagic,
          footerStartPos);
    }
  }
}

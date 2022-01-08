package com.markosindustries.distroboy.aws.s3.parquet;

import com.markosindustries.distroboy.parquet.RangeRequestingSeekableInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

public class PrefetchedFooterSeekableInputStream extends SeekableInputStream {
  private final RangeRequestingSeekableInputStream inner;
  private final byte[] prefetchedFooter;
  private final long footerStartPos;

  public PrefetchedFooterSeekableInputStream(
      RangeRequestingSeekableInputStream inner, byte[] prefetchedFooter, long footerStartPos) {
    this.inner = inner;
    this.prefetchedFooter = prefetchedFooter;
    this.footerStartPos = footerStartPos;
  }

  @Override
  public long getPos() throws IOException {
    return inner.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    inner.seek(newPos);
  }

  @Override
  public long skip(long n) throws IOException {
    return inner.skip(n);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    if (inner.getPos() >= footerStartPos) {
      final var footerIndex = (int) (inner.getPos() - footerStartPos);

      if (footerIndex + len <= prefetchedFooter.length) {
        System.arraycopy(prefetchedFooter, footerIndex, bytes, start, len);
        inner.seek(inner.getPos() + len);
        return;
      } else {
        throw new EOFException("Read extends past prefetched footer length");
      }
    }
    inner.readFully(bytes, start, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (inner.getPos() >= footerStartPos) {
      final var footerIndex = (int) (inner.getPos() - footerStartPos);

      final var bytesToRead = buf.remaining();
      if (footerIndex + bytesToRead <= prefetchedFooter.length) {
        buf.put(prefetchedFooter, footerIndex, bytesToRead);
        inner.seek(inner.getPos() + bytesToRead);
        return bytesToRead;
      } else {
        return -1;
      }
    }
    return inner.read(buf);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (inner.getPos() >= footerStartPos) {
      final var footerIndex = (int) (inner.getPos() - footerStartPos);

      final var bytesToRead = buf.remaining();
      if (footerIndex + bytesToRead <= prefetchedFooter.length) {
        buf.put(prefetchedFooter, footerIndex, bytesToRead);
        inner.seek(inner.getPos() + bytesToRead);
        return;
      } else {
        throw new EOFException("Read extends past prefetched footer length");
      }
    }
    inner.readFully(buf);
  }

  @Override
  public int read() throws IOException {
    if (inner.getPos() >= footerStartPos) {
      final var footerIndex = (int) (inner.getPos() - footerStartPos);
      if (footerIndex < prefetchedFooter.length) {
        final var footerByte = prefetchedFooter[footerIndex];
        inner.seek(inner.getPos() + 1);
        return Byte.toUnsignedInt(footerByte);
      } else {
        return -1;
      }
    }
    return inner.read();
  }
}

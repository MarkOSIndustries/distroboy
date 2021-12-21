package com.markosindustries.distroboy.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: document that this is for S3 basically, and example it up
public class RangeRequestingSeekableInputStream extends SeekableInputStream {
  private static Logger log = LoggerFactory.getLogger(RangeRequestingSeekableInputStream.class);

  private final RangeRetriever rangeRetriever;
  private final long bytesAvailable;
  private long position;

  @FunctionalInterface
  public interface RangeRetriever {
    InputStream retrieve(long position, long bytes);
  }

  public RangeRequestingSeekableInputStream(RangeRetriever rangeRetriever, long bytesAvailable) {
    this.rangeRetriever = rangeRetriever;
    this.bytesAvailable = bytesAvailable;
    this.position = 0L;
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    this.position = newPos;
  }

  @Override
  public long skip(long n) throws IOException {
    position += n;
    if (position < bytesAvailable) {
      return n;
    }
    return -1;
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    try (final var stream = rangeRetriever.retrieve(position, bytes.length)) {
      var arrayOffset = start;
      var bytesJustRead = 0;
      while ((arrayOffset < start + len)
          && (bytesJustRead = stream.read(bytes, arrayOffset, bytes.length)) != -1) {
        arrayOffset += bytesJustRead;
        position += bytesJustRead;
      }
      if (arrayOffset < start + len) {
        position = bytesAvailable;
        throw new EOFException("Not enough bytes remaining to satisfy read request");
      }
    }
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    try (final var stream = rangeRetriever.retrieve(position, buf.remaining())) {
      if (buf.hasArray()) {
        final var bytesRead =
            stream.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        if (bytesRead > -1) {
          buf.position(buf.position() + bytesRead);
          position += bytesRead;
        } else {
          position = bytesAvailable;
        }
        return bytesRead;
      } else {
        final var someBuffer = new byte[Math.min(300, buf.remaining())];
        int totalBytesRead = 0;
        while (buf.remaining() > 0) {
          final var bytesToRead = Math.min(someBuffer.length, buf.remaining());
          final var bytesRead = stream.read(someBuffer, 0, bytesToRead);
          if (bytesRead < 0) {
            position = bytesAvailable;
            return bytesRead;
          }
          totalBytesRead += bytesRead;
          position += bytesRead;
          buf.put(someBuffer, 0, bytesRead);
          if (bytesRead < bytesToRead) {
            break;
          }
        }
        return totalBytesRead;
      }
    }
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      final var bytesToRead = buf.remaining();
      readFully(buf.array(), buf.arrayOffset() + buf.position(), bytesToRead);
      position += bytesToRead;
      buf.position(buf.limit());
    } else {
      int bytesRead = 0;
      while (buf.remaining() > 0 && bytesRead > -1) {
        bytesRead = read(buf);
      }
    }
  }

  @Override
  public int read() throws IOException {
    log.warn("Single byte reads via range request are very inefficient!");
    try (final var stream = rangeRetriever.retrieve(position, 1)) {
      final var nextByte = stream.read();
      position++;
      return nextByte;
    }
  }
}

package distroboy.parquet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

class SimpleSeekableInputStream extends SeekableInputStream {
  private final RandomAccessFile file;

  SimpleSeekableInputStream(RandomAccessFile file) {
    this.file = file;
  }

  @Override
  public long getPos() throws IOException {
    return file.getFilePointer();
  }

  @Override
  public void seek(long newPos) throws IOException {
    file.seek(newPos);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    file.readFully(bytes);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    file.readFully(bytes, start, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      final var bytesRead =
          file.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
      if (bytesRead > -1) {
        buf.position(buf.position() + bytesRead);
      }
      return bytesRead;
    } else {
      final var someBuffer = new byte[Math.min(300, buf.remaining())];
      int totalBytesRead = 0;
      while (buf.remaining() > 0) {
        final var bytesToRead = Math.min(someBuffer.length, buf.remaining());
        final var bytesRead = file.read(someBuffer, 0, bytesToRead);
        if (bytesRead < 0) {
          return bytesRead;
        }
        totalBytesRead += bytesRead;
        buf.put(someBuffer, 0, bytesRead);
        if (bytesRead < bytesToRead) {
          break;
        }
      }
      return totalBytesRead;
    }
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      file.readFully(buf.array());
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
    return file.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return file.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return file.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n > Integer.MAX_VALUE) {
      return file.skipBytes(Integer.MAX_VALUE);
    } else {
      return file.skipBytes((int) n);
    }
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}

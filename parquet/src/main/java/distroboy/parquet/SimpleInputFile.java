package distroboy.parquet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

class SimpleInputFile implements InputFile {
  private final File file;

  SimpleInputFile(File file) {
    this.file = file;
  }

  @Override
  public long getLength() {
    return file.length();
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return new SimpleSeekableInputStream(new RandomAccessFile(file, "r"));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

package com.markosindustries.distroboy.parquet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class SimpleInputFile implements InputFile {
  private final File file;

  public SimpleInputFile(File file) {
    this.file = file;
  }

  @Override
  public long getLength() {
    return file.length();
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return new FileBasedSeekableInputStream(new RandomAccessFile(file, "r"));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

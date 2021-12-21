package com.markosindustries.distroboy.core.filesystem;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReadLinesFromFiles implements FlatMapOp<Path, String> {
  @Override
  public IteratorWithResources<String> flatMap(Path input) {
    try {
      return IteratorWithResources.from(Files.lines(input).iterator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

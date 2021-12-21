package com.markosindustries.distroboy.example.filesystem;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A FlatMap operation which takes a data set of paths and transforms it to a data set of Strings,
 * one for each line in the input file set. Mostly here as an example.
 */
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

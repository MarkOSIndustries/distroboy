package com.markosindustries.distroboy.example.filesystem;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataSource used mostly as an example. Not very useful for real distributed data processing,
 * unless you have some sort of mapped drive for your data set and can guarantee that each node will
 * consistently see the same file system contents at the same time. Will provide a data set with an
 * entry per path in the given directory.
 */
public class DirSource implements DataSource<Path> {
  private static final Logger log = LoggerFactory.getLogger(DirSource.class);
  private final String dirPath;

  public DirSource(String dirPath) {
    this.dirPath = dirPath;
  }

  private Stream<Path> paths() {
    try {
      final var dirAsPath = Path.of(dirPath);
      if (Files.exists(dirAsPath)) {
        return Files.list(dirAsPath);
      } else {
        log.error("Directory {} doesn't exist", dirPath);
        return Stream.empty();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long countOfFullSet() {
    return paths().count();
  }

  @Override
  public IteratorWithResources<Path> enumerateRangeOfFullSet(
      long startInclusive, long endExclusive) {
    return IteratorWithResources.from(
        paths().skip(startInclusive).limit(endExclusive - startInclusive).iterator());
  }
}

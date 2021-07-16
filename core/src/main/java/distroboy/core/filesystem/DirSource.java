package distroboy.core.filesystem;

import distroboy.core.operations.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public Iterator<Path> enumerateRangeOfFullSet(long startInclusive, long endExclusive) {
    return paths().skip(startInclusive).limit(endExclusive - startInclusive).iterator();
  }
}

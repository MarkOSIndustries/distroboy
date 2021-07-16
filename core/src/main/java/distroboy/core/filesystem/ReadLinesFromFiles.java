package distroboy.core.filesystem;

import distroboy.core.operations.FlatMapOp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class ReadLinesFromFiles implements FlatMapOp<Path, String> {
  @Override
  public Iterator<String> flatMap(Path input) {
    try {
      return Files.lines(input).iterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

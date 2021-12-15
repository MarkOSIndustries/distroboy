package distroboy.core.filesystem;

import distroboy.core.clustering.ClusterMemberId;
import java.nio.file.Path;

public class TempDir {
  private static final Path tempDir =
      Path.of(System.getProperty("java.io.tmpdir"), ClusterMemberId.self.toString());

  public static Path tempFile(String fileName) {
    return Path.of(tempDir.toAbsolutePath().toString(), fileName);
  }
}

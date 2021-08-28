package distroboy.coordinator;

import distroboy.core.Coordinator;
import distroboy.core.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Logging.configureDefault();
    Coordinator.startAndBlockUntilShutdown(7070);
  }
}

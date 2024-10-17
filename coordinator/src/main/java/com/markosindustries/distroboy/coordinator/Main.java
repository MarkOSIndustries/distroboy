package com.markosindustries.distroboy.coordinator;

import com.markosindustries.distroboy.core.Coordinator;
import com.markosindustries.distroboy.logback.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    Logging.configureDefault();
    Coordinator.startAndBlockUntilShutdown(7070);
  }
}

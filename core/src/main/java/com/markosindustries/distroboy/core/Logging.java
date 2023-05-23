package com.markosindustries.distroboy.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.LoggerFactory;

/** Support functions for configuring SLF4J to work the way distroboy wants to use it by default. */
public class Logging {
  private static final AtomicReference<Logging> INSTANCE = new AtomicReference<>();

  private final LoggerContext context;

  private Logging(LoggerContext context) {
    this.context = context;
  }

  public Logging setLevel(String logger, Level level) {
    context.getLogger(logger).setLevel(level);
    return this;
  }

  public Logging addAppender(Appender<ILoggingEvent> appender) {
    context.getLogger(Logger.ROOT_LOGGER_NAME).addAppender(makeConsoleAppender(context));
    return this;
  }

  /** Set up logging via SLF4J to work distroboy's default way */
  public static Logging configureDefault() {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    final var logging = new Logging(context);
    if (INSTANCE.compareAndSet(null, logging)) {
      context.reset();
      Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
      rootLogger.setLevel(Level.INFO);
      context.getLogger("io.grpc.netty").setLevel(Level.INFO);
      context.getLogger("io.netty").setLevel(Level.INFO);
      rootLogger.addAppender(makeConsoleAppender(context));
    }

    return INSTANCE.get();
  }

  private static ConsoleAppender<ILoggingEvent> makeConsoleAppender(LoggerContext loggerContext) {
    ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
    consoleAppender.setContext(loggerContext);
    consoleAppender.setName("console");
    LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
    encoder.setContext(loggerContext);

    PatternLayout layout = new PatternLayout();

    if (System.console() != null) {
      layout.setPattern(
          "%gray(%d{yyyy-MM-dd HH:mm:ss.SSS}) %5p %gray(---) %gray([%15.15t]) %cyan(%-40.40logger{39}) %gray(:) %m%n");
    } else {
      layout.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} %5p --- [%15.15t] %-40.40logger{39} : %m%n");
    }
    layout.setContext(loggerContext);
    layout.start();
    encoder.setLayout(layout);

    consoleAppender.setEncoder(encoder);
    consoleAppender.start();

    return consoleAppender;
  }
}

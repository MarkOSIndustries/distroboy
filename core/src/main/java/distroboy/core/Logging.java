package distroboy.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import org.slf4j.LoggerFactory;

public interface Logging {
  static void configureDefault() {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    context.reset();
    Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(Level.INFO);
    context.getLogger("distroboy").setLevel(Level.DEBUG);
    rootLogger.addAppender(makeConsoleAppender(context));
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

package net.pincette.jes.elastic;

import static java.text.MessageFormat.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import javax.json.JsonObject;
import net.pincette.jes.elastic.ElasticCommonSchema.Builder;
import net.pincette.jes.elastic.ElasticCommonSchema.EventBuilder;

public class LogHandler extends Handler {
  private static final String TRACE_ID = "traceId";

  private final ElasticCommonSchema ecs;
  private final Consumer<JsonObject> send;

  public LogHandler(final ElasticCommonSchema ecs, final Consumer<JsonObject> send) {
    this.ecs = ecs;
    this.send = send;
    setFilter(logRecord -> logRecord.getLevel().intValue() <= ecs.getLogLevel().intValue());
  }

  private static String action(final LogRecord logRecord) {
    return Optional.ofNullable(logRecord.getSourceClassName()).map(name -> name + ".").orElse("")
        + Optional.ofNullable(logRecord.getSourceMethodName()).orElse("Unknown method");
  }

  private static String message(final LogRecord logRecord) {
    return Optional.ofNullable(logRecord.getParameters())
        .map(parameters -> format(unformattedMessage(logRecord), removeTraceId(parameters)))
        .orElseGet(() -> unformattedMessage(logRecord));
  }

  private static Object[] removeTraceId(final Object[] parameters) {
    return stream(parameters).filter(p -> !p.toString().startsWith(TRACE_ID + "=")).toArray();
  }

  private static Optional<String> traceId(final LogRecord logRecord) {
    return ofNullable(logRecord.getParameters())
        .flatMap(
            parameters ->
                stream(parameters)
                    .map(Object::toString)
                    .map(s -> s.split("="))
                    .filter(parts -> parts.length == 2 && parts[0].equals(TRACE_ID))
                    .map(parts -> parts[1])
                    .findFirst());
  }

  private static String unformattedMessage(final LogRecord logRecord) {
    return Optional.ofNullable(logRecord.getResourceBundle())
        .filter(bundle -> bundle.containsKey(logRecord.getMessage()))
        .map(bundle -> bundle.getString(logRecord.getMessage()))
        .orElseGet(logRecord::getMessage);
  }

  private JsonObject logMessage(final LogRecord logRecord) {
    return ecs.builder()
        .addMessage(message(logRecord))
        .addTimestamp(ofEpochMilli(logRecord.getMillis()))
        .addLogLevel(logRecord.getLevel())
        .addIf(() -> traceId(logRecord), Builder::addTrace)
        .addEvent()
        .addCreated(ofEpochMilli(logRecord.getMillis()))
        .addOriginal(message(logRecord))
        .addSequence(logRecord.getSequenceNumber())
        .addAction(action(logRecord))
        .addSeverity(logRecord.getLevel().intValue())
        .addIf(
            b -> logRecord.getThrown() != null || logRecord.getLevel().equals(SEVERE),
            EventBuilder::addFailure)
        .build()
        .addIf(
            b -> logRecord.getThrown() != null,
            b -> b.addError().addThrowable(logRecord.getThrown()).build())
        .build();
  }

  @Override
  public void close() {
    // Nothing to do.
  }

  @Override
  public void flush() {
    // Nothing to do.
  }

  @Override
  public void publish(final LogRecord logRecord) {
    send.accept(logMessage(logRecord));
  }
}

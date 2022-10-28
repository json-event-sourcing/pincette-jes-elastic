package net.pincette.jes.elastic;

import static java.text.MessageFormat.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Stream.concat;
import static net.pincette.json.JsonUtil.merge;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.jes.elastic.ElasticCommonSchema.EventBuilder;

/**
 * This log handler produces JSON messages in the Elastic Common Schema format. You can add
 * parameters of <code>javax.json.JsonObject</code>. The names in the objects should follow the
 * Elastic Common Schema format. Those parameters are taken out before the normal message
 * formatting.
 *
 * @author Werner Donn\u00e9
 */
public class LogHandler extends Handler {
  private final ElasticCommonSchema ecs;
  private final Consumer<JsonObject> send;

  public LogHandler(final ElasticCommonSchema ecs, final Consumer<JsonObject> send) {
    this.ecs = ecs;
    this.send = send;
    setFilter(logRecord -> logRecord.getLevel().intValue() <= ecs.getLogLevel().intValue());
  }

  private static String action(final LogRecord logRecord) {
    return ofNullable(logRecord.getSourceClassName()).map(name -> name + ".").orElse("")
        + ofNullable(logRecord.getSourceMethodName()).orElse("Unknown method");
  }

  private static JsonObject addEcsFields(final JsonObject json, final LogRecord logRecord) {
    return merge(
        concat(
            Stream.of(json),
            ofNullable(logRecord.getParameters()).stream()
                .flatMap(Arrays::stream)
                .filter(JsonObject.class::isInstance)
                .map(JsonObject.class::cast)));
  }

  private static String message(final LogRecord logRecord) {
    return ofNullable(logRecord.getParameters())
        .filter(parameters -> parameters.length > 0)
        .map(parameters -> format(unformattedMessage(logRecord), removeEcsFields(parameters)))
        .orElseGet(() -> unformattedMessage(logRecord));
  }

  private static Object[] removeEcsFields(final Object[] parameters) {
    return stream(parameters).filter(p -> !(p instanceof JsonObject)).toArray();
  }

  private static String unformattedMessage(final LogRecord logRecord) {
    return ofNullable(logRecord.getResourceBundle())
        .filter(bundle -> bundle.containsKey(logRecord.getMessage()))
        .map(bundle -> bundle.getString(logRecord.getMessage()))
        .orElseGet(logRecord::getMessage);
  }

  private JsonObject logMessage(final LogRecord logRecord) {
    final String message = message(logRecord);

    return addEcsFields(
        ecs.builder()
            .addMessage(message)
            .addTimestamp(ofEpochMilli(logRecord.getMillis()))
            .addLogLevel(logRecord.getLevel())
            .addEvent()
            .addCreated(ofEpochMilli(logRecord.getMillis()))
            .addOriginal(message)
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
            .build(),
        logRecord);
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

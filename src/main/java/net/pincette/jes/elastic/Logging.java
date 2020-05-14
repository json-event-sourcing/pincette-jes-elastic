package net.pincette.jes.elastic;

import static java.text.MessageFormat.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static net.pincette.jes.elastic.Util.errorMessage;
import static net.pincette.jes.elastic.Util.sendMessage;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.STATUS_CODE;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.getUsername;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.Validate.hasErrors;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import net.pincette.jes.Aggregate;
import net.pincette.jes.elastic.ElasticCommonSchema.EventBuilder;
import net.pincette.jes.util.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Elasticsearch logging utilities for JSON Event Sourcing.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Logging {
  private static final String ANONYMOUS = "anonymous";
  private static final String COMMAND_ERROR = "COMMAND_ERROR";
  private static final String ECS_COMMAND = "command";
  private static final String EVENT = "event";
  private static final String EXCEPTION = "exception";
  private static final String UNKNOWN = "unknown";

  private Logging() {}

  private static ElasticCommonSchema.Builder aggregateBuilder(
      final Aggregate aggregate, final String serviceVersion) {
    return new ElasticCommonSchema()
        .withApp(aggregate.app())
        .withEnvironment(aggregate.environment())
        .withLogLevel(INFO)
        .withService(fullType(aggregate))
        .withServiceVersion(serviceVersion)
        .builder();
  }

  /**
   * Converts a command to an Elastic Common Schema message.
   *
   * @param command the command.
   * @param aggregate the aggregate for which the command is meant.
   * @param serviceVersion the version of the aggregate.
   * @return The ECM message.
   * @since 1.1
   */
  public static JsonObject command(
      final JsonObject command, final Aggregate aggregate, final String serviceVersion) {
    final boolean errors = hasErrors(command);

    return aggregateBuilder(aggregate, serviceVersion)
        .addMessage(commandMessage(command))
        .addTrace(trace(command))
        .addUser(username(command))
        .addTimestamp(timestamp(command))
        .addEvent()
        .addDataset(ECS_COMMAND)
        .addOriginal(string(command, false))
        .addAction(command.getString(COMMAND))
        .addIf(
            b -> errors,
            b -> b.addCode(command.containsKey(EXCEPTION) ? SEVERE.getName() : COMMAND_ERROR))
        .addIf(b -> errors, EventBuilder::addFailure)
        .build()
        .addIf(
            b -> errors,
            b ->
                b.addError()
                    .addCode(commandCode(command))
                    .addMessage(errorMessage(command))
                    .build())
        .build();
  }

  private static String commandCode(final JsonObject command) {
    return Optional.ofNullable(command.getJsonNumber(STATUS_CODE))
        .map(JsonNumber::longValue)
        .orElse(0L)
        .toString();
  }

  private static String commandMessage(final JsonObject command) {
    return "Command " + commandSuffix(command);
  }

  private static String commandSuffix(final JsonObject json) {
    return json.getString(COMMAND)
        + " for aggregate "
        + json.getString(ID)
        + " of type "
        + json.getString(TYPE);
  }

  private static String createUri(final String uri) {
    return uri + (uri.endsWith("/") ? "" : "/") + randomUUID().toString();
  }

  /**
   * Converts an event to an Elastic Common Schema message.
   *
   * @param event the event.
   * @param aggregate the aggregate that produced the event.
   * @param serviceVersion the version of the aggregate.
   * @return The ECS message.
   * @since 1.1
   */
  public static JsonObject event(
      final JsonObject event, final Aggregate aggregate, final String serviceVersion) {
    return aggregateBuilder(aggregate, serviceVersion)
        .addMessage(eventMessage(event))
        .addTrace(trace(event))
        .addUser(username(event))
        .addTimestamp(timestamp(event))
        .addEvent()
        .addDataset(EVENT)
        .addOriginal(string(event, false))
        .addAction(event.getString(COMMAND))
        .build()
        .build();
  }

  private static String eventMessage(final JsonObject event) {
    return "Event generated by command " + commandSuffix(event);
  }

  private static String fullType(final Aggregate aggregate) {
    return aggregate.app() + "-" + aggregate.type();
  }

  /**
   * Logs commands and events for <code>aggregate</code>. When the log level is at least <code>INFO
   * </code> all documents appearing on the command and events streams are sent to the log index.
   * When the log level is at least <code>SEVERE</code> all commands with validation errors are sent
   * to the log index.
   *
   * @param aggregate the given aggregate.
   * @param level the log level.
   * @param uri the URI of the Elasticsearch index.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void log(
      final Aggregate aggregate,
      final Level level,
      final String uri,
      final String authorizationHeader) {
    log(aggregate, level, null, uri, authorizationHeader);
  }

  /**
   * Logs commands and events for <code>aggregate</code>. When the log level is at least <code>INFO
   * </code> all documents appearing on the command and events streams are sent to the log index.
   * When the log level is at least <code>SEVERE</code> all commands with validation errors are sent
   * to the log index.
   *
   * @param aggregate the given aggregate.
   * @param level the log level.
   * @param serviceVersion the version of the service.
   * @param uri the URI of the Elasticsearch index.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void log(
      final Aggregate aggregate,
      final Level level,
      final String serviceVersion,
      final String uri,
      final String authorizationHeader) {
    if (level.intValue() <= INFO.intValue()) {
      aggregate
          .commands()
          .mapValues(
              v ->
                  send(
                      string(command(v, aggregate, serviceVersion), false),
                      uri,
                      authorizationHeader));
      aggregate
          .events()
          .mapValues(
              v ->
                  send(
                      string(event(v, aggregate, serviceVersion), false),
                      uri,
                      authorizationHeader));
    }

    if (level.intValue() <= SEVERE.intValue()) {
      aggregate
          .replies()
          .filter((k, v) -> hasErrors(v))
          .mapValues(
              v ->
                  send(
                      string(command(v, aggregate, serviceVersion), false),
                      uri,
                      authorizationHeader));
    }
  }

  /**
   * Sends all log entries appearing in <code>logger</code> to an Elasticsearch index using the
   * Elastic Common Schema.
   *
   * @param logger the given logger.
   * @param uri the URI of the Elasticsearch index.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.2.4
   */
  public static void log(final Logger logger, final String uri, final String authorizationHeader) {
    log(logger, logger.getLevel(), null, null, uri, authorizationHeader);
  }

  /**
   * Sends all log entries appearing in <code>logger</code> to an Elasticsearch index using the
   * Elastic Common Schema.
   *
   * @param logger the given logger.
   * @param level the log level.
   * @param uri the URI of the Elasticsearch index.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void log(
      final Logger logger, final Level level, final String uri, final String authorizationHeader) {
    log(logger, level, null, null, uri, authorizationHeader);
  }

  /**
   * Sends all log entries appearing in <code>logger</code> to an Elasticsearch index using the
   * Elastic Common Schema.
   *
   * @param logger the given logger.
   * @param level the log level.
   * @param serviceVersion the version of the service.
   * @param environment the name of the environment, e.g. "dev", "acc", "prod", etc.
   * @param uri the URI of the Elasticsearch index.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void log(
      final Logger logger,
      final Level level,
      final String serviceVersion,
      final String environment,
      final String uri,
      final String authorizationHeader) {
    logger.addHandler(
        new LogHandler(
            new ElasticCommonSchema()
                .withApp(logger.getName())
                .withLogLevel(level)
                .withService(logger.getName())
                .withServiceVersion(serviceVersion)
                .withEnvironment(environment),
            json -> send(string(json), uri, authorizationHeader)));
  }

  /**
   * Sends all log entries appearing in <code>logger</code> to an Elasticsearch index using the
   * Elastic Common Schema.
   *
   * @param logger the given logger.
   * @param serviceVersion the version of the service.
   * @param environment the name of the environment, e.g. "dev", "acc", "prod", etc.
   * @param producer the Kafka producer.
   * @param logTopic the Kafka topic to publish the log messages on.
   * @since 1.2.4
   */
  public static void log(
      final Logger logger,
      final String serviceVersion,
      final String environment,
      final KafkaProducer<String, JsonObject> producer,
      final String logTopic) {
    log(logger, logger.getLevel(), serviceVersion, environment, producer, logTopic);
  }

  /**
   * Sends all log entries appearing in <code>logger</code> to an Elasticsearch index using the
   * Elastic Common Schema.
   *
   * @param logger the given logger.
   * @param level the log level.
   * @param serviceVersion the version of the service.
   * @param environment the name of the environment, e.g. "dev", "acc", "prod", etc.
   * @param producer the Kafka producer.
   * @param logTopic the Kafka topic to publish the log messages on.
   * @since 1.1.3
   */
  public static void log(
      final Logger logger,
      final Level level,
      final String serviceVersion,
      final String environment,
      final KafkaProducer<String, JsonObject> producer,
      final String logTopic) {
    logger.addHandler(
        new LogHandler(
            new ElasticCommonSchema()
                .withApp(logger.getName())
                .withLogLevel(level)
                .withService(logger.getName())
                .withServiceVersion(serviceVersion)
                .withEnvironment(environment),
            json ->
                Kafka.send(
                    producer, new ProducerRecord<>(logTopic, randomUUID().toString(), json))));
  }

  /**
   * Logs commands and events for <code>aggregate</code>. When the log level is at least <code>INFO
   * </code> all documents appearing on the command and events streams are sent to the log topic.
   * When the log level is at least <code>SEVERE</code> all commands with validation errors are sent
   * to the log topic.
   *
   * @param aggregate the given aggregate.
   * @param level the log level.
   * @param serviceVersion the version of the service.
   * @param logTopic the Kafka topic for logging.
   * @since 1.1.2
   */
  public static void logKafka(
      final Aggregate aggregate,
      final Level level,
      final String serviceVersion,
      final String logTopic) {
    if (level.intValue() <= INFO.intValue()) {
      aggregate.commands().mapValues(v -> command(v, aggregate, serviceVersion)).to(logTopic);
      aggregate.events().mapValues(v -> event(v, aggregate, serviceVersion)).to(logTopic);
    }

    if (level.intValue() <= SEVERE.intValue()) {
      aggregate
          .replies()
          .filter((k, v) -> hasErrors(v))
          .mapValues(v -> command(v, aggregate, serviceVersion))
          .to(logTopic);
    }
  }

  private static CompletionStage<Boolean> send(
      final String message, final String uri, final String authorizationHeader) {
    return sendMessage(message, createUri(uri), "PUT", "application/json", authorizationHeader);
  }

  private static Instant timestamp(final JsonObject json) {
    return Optional.ofNullable(json.getJsonNumber(TIMESTAMP))
        .map(JsonNumber::longValue)
        .map(Instant::ofEpochMilli)
        .orElseGet(Instant::now);
  }

  private static String trace(final JsonObject json) {
    return json.getString(CORR, UNKNOWN);
  }

  private static String username(final JsonObject json) {
    return getUsername(json).orElse(ANONYMOUS);
  }

  private static class LogHandler extends Handler {
    private final Consumer<JsonObject> send;
    private ElasticCommonSchema ecs;

    private LogHandler(final ElasticCommonSchema ecs, final Consumer<JsonObject> send) {
      this.ecs = ecs;
      this.send = send;
      setFilter(record -> record.getLevel().intValue() <= ecs.getLogLevel().intValue());
    }

    private static String action(final LogRecord record) {
      return Optional.ofNullable(record.getSourceClassName()).map(name -> name + ".").orElse("")
          + Optional.ofNullable(record.getSourceMethodName()).orElse("Unknown method");
    }

    private static String message(final LogRecord record) {
      return Optional.ofNullable(record.getParameters())
          .map(parameters -> format(unformattedMessage(record), parameters))
          .orElseGet(() -> unformattedMessage(record));
    }

    private static String unformattedMessage(final LogRecord record) {
      return Optional.ofNullable(record.getResourceBundle())
          .filter(bundle -> bundle.containsKey(record.getMessage()))
          .map(bundle -> bundle.getString(record.getMessage()))
          .orElseGet(record::getMessage);
    }

    private JsonObject logMessage(final LogRecord record) {
      return ecs.builder()
          .addMessage(message(record))
          .addTimestamp(ofEpochMilli(record.getMillis()))
          .addLogLevel(record.getLevel())
          .addEvent()
          .addCreated(ofEpochMilli(record.getMillis()))
          .addOriginal(message(record))
          .addSequence(record.getSequenceNumber())
          .addAction(action(record))
          .addSeverity(record.getLevel().intValue())
          .addIf(b -> record.getThrown() != null, EventBuilder::addFailure)
          .build()
          .addIf(
              b -> record.getThrown() != null,
              b -> b.addError().addThrowable(record.getThrown()).build())
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
    public void publish(final LogRecord record) {
      send.accept(logMessage(record));
    }
  }
}

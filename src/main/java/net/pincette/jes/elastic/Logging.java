package net.pincette.jes.elastic;

import static java.lang.String.valueOf;
import static java.text.MessageFormat.format;
import static java.time.Instant.now;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.util.Arrays.stream;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.elastic.Util.errorMessage;
import static net.pincette.jes.elastic.Util.sendMessage;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.STATUS_CODE;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.getUsername;
import static net.pincette.util.Json.addIf;
import static net.pincette.util.Json.hasErrors;
import static net.pincette.util.Json.string;
import static net.pincette.util.Util.getStackTrace;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import net.pincette.jes.Aggregate;

/**
 * Elasticsearch logging utilities for JSON Event Sourcing.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Logging {
  private static final String ECS = "ecs";
  private static final String ECS_ACTION = "action";
  private static final String ECS_AGENT = "agent";
  private static final String ECS_APPLICATION = "application";
  private static final String ECS_CATEGORY = "category";
  private static final String ECS_CODE = "code";
  private static final String ECS_COMMAND = "command";
  private static final String ECS_CREATED = "created";
  private static final String ECS_ENVIRONMENT = "environment";
  private static final String ECS_ERROR = "SEVERE";
  private static final String ECS_EVENT = "event";
  private static final String ECS_HOST = "host";
  private static final String ECS_ID = "id";
  private static final String ECS_INFO = "INFO";
  private static final String ECS_KIND = "kind";
  private static final String ECS_LABELS = "labels";
  private static final String ECS_LEVEL = "level";
  private static final String ECS_LOG = "log";
  private static final String ECS_MESSAGE = "message";
  private static final String ECS_NAME = "name";
  private static final String ECS_ORIGINAL = "original";
  private static final String ECS_OS = "os";
  private static final String ECS_SERVICE = "service";
  private static final String ECS_SEVERITY = "severity";
  private static final String ECS_TAGS = "tags";
  private static final String ECS_TIMESTAMP = "@timestamp";
  private static final String ECS_TIMEZONE = "timezone";
  private static final String ECS_TYPE = "type";
  private static final String ECS_USER = "user";
  private static final String ECS_VERSION = "version";
  private static final String VERSION = "1.0";

  private Logging() {}

  private static String command(
      final JsonObject command, final Aggregate aggregate, final String serviceVersion) {
    return string(
        ecsGeneral(command, aggregate, serviceVersion)
            .add(ECS_MESSAGE, commandMessage(command))
            .add(ECS_EVENT, ecsCommandEvent(command))
            .add(ECS_LOG, ecsLog(ECS_INFO))
            .build(),
        false);
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

  private static JsonObjectBuilder ecsCommandEvent(final JsonObject command) {
    return ecsEvent(command)
        .add(ECS_KIND, ECS_COMMAND)
        .add(ECS_CATEGORY, ECS_COMMAND)
        .add(ECS_ACTION, command.getString(COMMAND));
  }

  private static JsonObjectBuilder ecsError(final JsonObject command) {
    return createObjectBuilder()
        .add(ECS_MESSAGE, errorMessage(command))
        .add(
            ECS_CODE,
            Optional.ofNullable(command.getJsonNumber(STATUS_CODE))
                .map(JsonNumber::longValue)
                .orElse(0L));
  }

  private static JsonObjectBuilder ecsEvent(final JsonObject json) {
    return createObjectBuilder()
        .add(ECS_CREATED, ofEpochMilli(json.getJsonNumber(TIMESTAMP).longValue()).toString())
        .add(ECS_ORIGINAL, string(json, false))
        .add(ECS_TIMEZONE, systemDefault().toString());
  }

  private static JsonObjectBuilder ecsGeneral(
      final JsonObject json, final Aggregate aggregate, final String serviceVersion) {
    return createObjectBuilder()
        .add(ECS_TIMESTAMP, now().toString())
        .add(ECS_LABELS, ecsLabels(aggregate))
        .add(ECS_TAGS, ecsTags(aggregate))
        .add(ECS_SERVICE, ecsService(aggregate, serviceVersion))
        .add(ECS_AGENT, ecsService(aggregate, serviceVersion))
        .add(ECS, ecsVersion())
        .add(ECS_HOST, ecsHost(json));
  }

  private static JsonObjectBuilder ecsHost(final JsonObject json) {
    return ecsHost().add(ECS_USER, ecsUser(json));
  }

  private static JsonObjectBuilder ecsHost() {
    return createObjectBuilder().add(ECS_OS, ecsOs());
  }

  private static JsonObjectBuilder ecsLabels(final String app, final String environment) {
    return createObjectBuilder().add(ECS_APPLICATION, app).add(ECS_ENVIRONMENT, environment);
  }

  private static JsonObjectBuilder ecsLabels(final Aggregate aggregate) {
    return ecsLabels(aggregate.app(), aggregate.environment());
  }

  private static JsonObjectBuilder ecsLog(final String level) {
    return createObjectBuilder().add(ECS_LEVEL, level);
  }

  private static JsonObjectBuilder ecsOs() {
    return createObjectBuilder()
        .add(ECS_NAME, System.getProperty("os.name"))
        .add(ECS_VERSION, System.getProperty("os.version"));
  }

  private static JsonArrayBuilder ecsTags(final String[] tags) {
    return stream(tags).reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
  }

  private static JsonArrayBuilder ecsTags(final Aggregate aggregate) {
    return ecsTags(new String[] {aggregate.environment()});
  }

  private static JsonObjectBuilder ecsService(
      final Aggregate aggregate, final String serviceVersion) {
    return createObjectBuilder()
        .add(ECS_NAME, fullType(aggregate))
        .add(ECS_TYPE, "pincette-jes")
        .add(ECS_VERSION, serviceVersion != null ? serviceVersion : "unknown");
  }

  private static JsonObjectBuilder ecsUser(final JsonObject json) {
    return createObjectBuilder().add(ECS_NAME, getUsername(json).orElse("anonymous"));
  }

  private static JsonObjectBuilder ecsVersion() {
    return createObjectBuilder().add(ECS_VERSION, "1.0.0");
  }

  private static String error(
      final JsonObject command, final Aggregate aggregate, final String serviceVersion) {
    return string(
        ecsGeneral(command, aggregate, serviceVersion)
            .add(ECS_MESSAGE, commandMessage(command))
            .add(ECS_EVENT, ecsCommandEvent(command))
            .add(ECS_LOG, ecsLog(ECS_ERROR))
            .add(ECS_ERROR, ecsError(command))
            .build(),
        false);
  }

  private static String event(
      final JsonObject event, final Aggregate aggregate, final String serviceVersion) {
    return string(
        ecsGeneral(event, aggregate, serviceVersion)
            .add(ECS_MESSAGE, eventMessage(event))
            .add(
                ECS_EVENT,
                ecsEvent(event)
                    .add(ECS_KIND, ECS_EVENT)
                    .add(ECS_CATEGORY, ECS_EVENT)
                    .add(ECS_ACTION, event.getString(COMMAND)))
            .add(ECS_LOG, ecsLog(ECS_INFO))
            .build(),
        false);
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
          .mapValues(v -> send(command(v, aggregate, serviceVersion), uri, authorizationHeader));
      aggregate
          .events()
          .mapValues(v -> send(event(v, aggregate, serviceVersion), uri, authorizationHeader));
    }

    if (level.intValue() <= SEVERE.intValue()) {
      aggregate
          .replies()
          .filter((k, v) -> hasErrors(v))
          .mapValues(v -> send(error(v, aggregate, serviceVersion), uri, authorizationHeader));
    }
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
    logger.addHandler(new LogHandler(level, serviceVersion, environment, uri, authorizationHeader));
  }

  private static CompletionStage<Boolean> send(
      final String message, final String uri, final String authorizationHeader) {
    return sendMessage(message, createUri(uri), "PUT", "application/json", authorizationHeader);
  }

  private static class LogHandler extends Handler {
    private final String authorizationHeader;
    private final String environment;
    private final String serviceVersion;
    private final String uri;

    private LogHandler(
        final Level level,
        final String serviceVersion,
        final String environment,
        final String uri,
        final String authorizationHeader) {
      this.environment = environment;
      this.serviceVersion = serviceVersion;
      this.uri = uri;
      this.authorizationHeader = authorizationHeader;
      setFilter(record -> record.getLevel().intValue() <= level.intValue());
    }

    private static String action(final LogRecord record) {
      return Optional.ofNullable(record.getSourceClassName()).map(name -> name + ".").orElse("")
          + Optional.ofNullable(record.getSourceMethodName()).orElse("Unknown method");
    }

    private static JsonObjectBuilder ecsError(final LogRecord record) {
      return createObjectBuilder()
          .add(ECS_MESSAGE, getStackTrace(record.getThrown()))
          .add(ECS_CODE, "exception");
    }

    private static JsonObjectBuilder ecsEvent(final LogRecord record) {
      return createObjectBuilder()
          .add(ECS_CREATED, ofEpochMilli(record.getMillis()).toString())
          .add(ECS_ID, valueOf(record.getSequenceNumber()))
          .add(ECS_CATEGORY, "logger")
          .add(ECS_ACTION, action(record))
          .add(ECS_SEVERITY, record.getLevel().intValue())
          .add(ECS_ORIGINAL, message(record))
          .add(ECS_TIMEZONE, systemDefault().toString());
    }

    private static JsonObjectBuilder ecsGeneral(
        final LogRecord record, final String serviceVersion, final String environment) {
      return createObjectBuilder()
          .add(ECS_TIMESTAMP, ofEpochMilli(record.getMillis()).toString())
          .add(ECS_SERVICE, ecsService(record, serviceVersion))
          .add(ECS_AGENT, ecsService(record, serviceVersion))
          .add(ECS_TAGS, ecsTags(new String[] {environment}))
          .add(ECS_LABELS, ecsLabels(record.getLoggerName(), environment))
          .add(ECS, ecsVersion())
          .add(ECS_HOST, ecsHost());
    }

    private static JsonObjectBuilder ecsService(
        final LogRecord record, final String serviceVersion) {
      return createObjectBuilder()
          .add(ECS_NAME, record.getLoggerName())
          .add(ECS_TYPE, "pincette-jes")
          .add(ECS_VERSION, serviceVersion != null ? serviceVersion : "unknown");
    }

    private static String logMessage(
        final LogRecord record, final String serviceVersion, final String environment) {
      return string(
          addIf(
                  ecsGeneral(record, serviceVersion, environment)
                      .add(ECS_MESSAGE, message(record))
                      .add(ECS_EVENT, ecsEvent(record))
                      .add(ECS_LOG, ecsLog(record.getLevel().toString())),
                  () -> record.getThrown() != null,
                  b -> b.add(ECS_ERROR, ecsError(record)))
              .build());
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
      send(logMessage(record, serviceVersion, environment), uri, authorizationHeader);
    }
  }
}

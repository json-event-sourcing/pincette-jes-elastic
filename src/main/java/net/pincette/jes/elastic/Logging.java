package net.pincette.jes.elastic;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static net.pincette.jes.JsonFields.COMMAND;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.JsonFields.STATUS_CODE;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.jes.JsonFields.TYPE;
import static net.pincette.jes.Util.getUsername;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.Validate.hasErrors;
import static net.pincette.util.Or.tryWith;

import java.time.Instant;
import java.util.Optional;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import net.pincette.jes.Aggregate;
import net.pincette.jes.elastic.ElasticCommonSchema.EventBuilder;

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
  private static final String MESSAGE = "message";
  private static final String UNKNOWN = "unknown";

  private Logging() {}

  private static <T, U> ElasticCommonSchema.Builder aggregateBuilder(
      final Aggregate<T, U> aggregate, final String serviceVersion) {
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
  public static <T, U> JsonObject command(
      final JsonObject command, final Aggregate<T, U> aggregate, final String serviceVersion) {
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

  private static String errorMessage(final JsonObject command) {
    return tryWith(() -> command.getString(MESSAGE, null))
        .or(() -> command.getString(EXCEPTION, null))
        .or(() -> "Validation error")
        .get()
        .orElse("Unknown error");
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
  public static <T, U> JsonObject event(
      final JsonObject event, final Aggregate<T, U> aggregate, final String serviceVersion) {
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

  private static <T, U> String fullType(final Aggregate<T, U> aggregate) {
    return aggregate.app() + "-" + aggregate.type();
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
}

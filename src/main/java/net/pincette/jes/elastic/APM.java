package net.pincette.jes.elastic;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.toHexString;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.MonitorSteps.ERROR;
import static net.pincette.jes.MonitorSteps.allError;
import static net.pincette.jes.MonitorSteps.allOk;
import static net.pincette.jes.elastic.Util.sendMessage;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Json.string;
import static net.pincette.util.Pair.pair;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.Aggregate;
import net.pincette.util.Json;
import net.pincette.util.Pair;
import org.apache.kafka.streams.KeyValue;

/**
 * Connects a JSON Event Sourcing aggregate to the Elasticsearch APM service.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class APM {
  private static final String AFTER = "after";
  private static final String APM_AGENT = "agent";
  private static final String APM_DURATION = "duration";
  private static final String APM_ID = "id";
  private static final String APM_LEVEL = "level";
  private static final String APM_LOG = "log";
  private static final String APM_MESSAGE = "message";
  private static final String APM_METADATA = "metadata";
  private static final String APM_NAME = "name";
  private static final String APM_PARENT = "parent";
  private static final String APM_PARENT_ID = "parent_id";
  private static final String APM_REQUEST = "request";
  private static final String APM_SERVICE = "service";
  private static final String APM_SPAN = "span";
  private static final String APM_SPAN_COUNT = "span_count";
  private static final String APM_START = "start";
  private static final String APM_STARTED = "started";
  private static final String APM_SYNC = "sync";
  private static final String APM_TRACE_ID = "trace_id";
  private static final String APM_TRANSACTION = "transaction";
  private static final String APM_TRANSACTION_ID = "transaction_id";
  private static final String APM_TYPE = "type";
  private static final String COMMAND = "command";
  private static final String STEP = "step";
  private static final String STEPS = "steps";
  private static final String TIMESTAMP = "timestamp";

  private APM() {}

  private static Map<String, JsonObject> byStep(final JsonArray steps) {
    return getSteps(steps)
        .collect(toMap(step -> step.getString(STEP), step -> step, (v1, v2) -> v1));
  }

  private static String createError(final JsonArray steps) {
    return getSteps(steps)
        .filter(s -> ERROR.equals(s.getString(STEP, null)))
        .findFirst()
        .map(
            error ->
                string(
                    createObjectBuilder()
                        .add(
                            ERROR,
                            createObjectBuilder()
                                .add(APM_ID, generateId())
                                .add(TIMESTAMP, timestamp(error))
                                .add(
                                    APM_LOG,
                                    createObjectBuilder()
                                        .add(APM_LEVEL, "ERROR")
                                        .add(APM_MESSAGE, getMessage(error))))
                        .build(),
                    false))
        .orElse(null);
  }

  private static String createErrorMessage(final Aggregate aggregate, final JsonArray steps) {
    return createMetadata(aggregate) + "\n" + createError(steps) + "\n";
  }

  private static String createMessage(final Aggregate aggregate, final JsonArray steps) {
    final Map<String, JsonObject> byStep = byStep(steps);
    final String traceId = generateId() + generateId();
    final String transactionId = generateId();

    return createMetadata(aggregate)
        + "\n"
        + createSpans(transactionId, traceId, byStep)
        + "\n"
        + createTransaction(transactionId, traceId, byStep.values())
        + "\n";
  }

  private static String createMetadata(final Aggregate aggregate) {
    return string(
        createObjectBuilder()
            .add(
                APM_METADATA,
                createObjectBuilder()
                    .add(
                        APM_SERVICE,
                        createObjectBuilder()
                            .add(APM_AGENT, createObjectBuilder().add(APM_NAME, "pincette-jes-apm"))
                            .add(APM_NAME, name(aggregate))))
            .build(),
        false);
  }

  private static String createSpan(
      final String transactionId,
      final String traceId,
      final JsonObject step,
      final JsonObject after,
      final long first) {
    return string(
        createObjectBuilder()
            .add(
                APM_SPAN,
                createObjectBuilder()
                    .add(APM_ID, generateId())
                    .add(APM_TRACE_ID, traceId)
                    .add(APM_TRANSACTION_ID, transactionId)
                    .add(APM_PARENT_ID, transactionId)
                    .add(APM_NAME, step.getString(STEP))
                    .add(APM_PARENT, 1)
                    .add(APM_START, timestamp(after) - first)
                    .add(APM_TYPE, "pincette-jes")
                    .add(APM_SYNC, false)
                    .add(APM_DURATION, timestamp(step) - timestamp(after)))
            .build(),
        false);
  }

  private static String createSpans(
      final String transactionId, final String traceId, final Map<String, JsonObject> byStep) {
    final long first = start(byStep.values());

    return byStep.values().stream()
        .filter(step -> step.containsKey(AFTER))
        .map(
            step ->
                createSpan(transactionId, traceId, step, byStep.get(step.getString(AFTER)), first))
        .collect(Collectors.joining("\n"));
  }

  private static String createTransaction(
      final String transactionId, final String traceId, final Collection<JsonObject> steps) {
    return string(
        createObjectBuilder()
            .add(
                APM_TRANSACTION,
                createObjectBuilder()
                    .add(APM_NAME, getCommand(steps).orElse("NO COMMAND"))
                    .add(APM_ID, transactionId)
                    .add(APM_TRACE_ID, traceId)
                    .add(APM_DURATION, getTotalDuration(steps))
                    .add(APM_TYPE, APM_REQUEST)
                    .add(APM_SPAN_COUNT, createObjectBuilder().add(APM_STARTED, steps.size() - 1)))
            .build(),
        false);
  }

  private static String generateId() {
    return toHexString(new Random().nextLong());
  }

  private static Set<String> getAllSteps(final JsonArray steps) {
    return getSteps(steps)
        .map(step -> step.getString(STEP, null))
        .filter(Objects::nonNull)
        .collect(toSet());
  }

  private static Optional<String> getCommand(final Collection<JsonObject> steps) {
    return steps.stream()
        .map(step -> step.getString(COMMAND, null))
        .filter(Objects::nonNull)
        .findFirst();
  }

  private static String getMessage(final JsonObject error) {
    return Optional.ofNullable(error.getJsonObject(COMMAND))
        .map(Util::errorMessage)
        .orElse("Unknown error");
  }

  private static Stream<JsonObject> getSteps(final JsonArray steps) {
    return steps.stream().filter(Json::isObject).map(JsonValue::asJsonObject);
  }

  private static long getTotalDuration(final Collection<JsonObject> steps) {
    final Pair<Long, Long> pair =
        steps.stream()
            .map(APM::timestamp)
            .reduce(
                pair(MAX_VALUE, 0L),
                (p, v) -> pair(v < p.first ? v : p.first, v > p.second ? v : p.second),
                (p1, p2) -> p1);

    return pair.second - pair.first;
  }

  private static boolean hasStep(final JsonArray steps, final String step) {
    return getSteps(steps).anyMatch(s -> step.equals(s.getString(STEP, null)));
  }

  private static boolean isComplete(final JsonArray steps) {
    final Set<String> all = getAllSteps(steps);

    return intersection(all, allOk()).size() == allOk().size()
        || intersection(all, allError()).size() == allError().size();
  }

  /**
   * Takes the monitor stream from <code>aggregate</code> and sends a message to APM for each
   * message.
   *
   * @param aggregate the given aggregate.
   * @param uri the APM endpoint.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void monitor(
      final Aggregate aggregate, final String uri, final String authorizationHeader) {
    aggregate
        .monitor()
        .filter((k, v) -> v.containsKey(STEP) && v.containsKey(TIMESTAMP))
        .groupByKey()
        .aggregate(
            () -> createObjectBuilder().add(STEPS, createArrayBuilder()).build(),
            (k, v, json) ->
                !hasStep(json.getJsonArray(STEPS), v.getString(STEP))
                    ? createObjectBuilder(json)
                        .add(STEPS, createArrayBuilder(json.getJsonArray(STEPS)).add(v))
                        .build()
                    : json)
        .toStream()
        .filter((k, v) -> isComplete(v.getJsonArray(STEPS)))
        .map(
            (k, v) ->
                new KeyValue<>(
                    k,
                    sendMessage(
                        Optional.ofNullable(v.getJsonArray(STEPS))
                            .map(
                                steps ->
                                    hasStep(steps, ERROR)
                                        ? createErrorMessage(aggregate, steps)
                                        : createMessage(aggregate, steps))
                            .orElse(null),
                        uri,
                        "POST",
                        "application/x-ndjson",
                        authorizationHeader)));
  }

  private static String name(final Aggregate aggregate) {
    return aggregate.app() + "-" + aggregate.type() + "-" + aggregate.environment();
  }

  private static long start(final Collection<JsonObject> steps) {
    return steps.stream()
        .reduce(
            MAX_VALUE,
            (r, v) -> Optional.of(timestamp(v)).filter(t -> t < r).orElse(r),
            (r1, r2) -> r1);
  }

  private static long timestamp(final JsonObject step) {
    return step.getJsonNumber(TIMESTAMP).longValue();
  }
}

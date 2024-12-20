package net.pincette.jes.elastic;

import static java.time.Instant.now;
import static java.time.ZoneId.systemDefault;
import static java.util.Arrays.stream;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.util.Util.getStackTrace;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * This lets you build JSON messages according to the Elastic Common Schema.
 *
 * @see <a href="https://www.elastic.co/guide/en/ecs/current/ecs-field-reference.html">ECS Field
 *     Reference</a>
 * @author Werner Donné
 * @since 1.1
 */
public class ElasticCommonSchema {
  private static final String ECS = "ecs";
  private static final String ECS_ACTION = "action";
  private static final String ECS_AGENT = "agent";
  private static final String ECS_APPLICATION = "application";
  private static final String ECS_BODY = "body";
  private static final String ECS_BYTES = "bytes";
  private static final String ECS_CATEGORY = "category";
  private static final String ECS_CODE = "code";
  private static final String ECS_CONTENT = "content";
  private static final String ECS_CREATED = "created";
  private static final String ECS_DATASET = "dataset";
  private static final String ECS_DOMAIN = "domain";
  private static final String ECS_DURATION = "duration";
  private static final String ECS_END = "end";
  private static final String ECS_ENVIRONMENT = "environment";
  private static final String ECS_ERROR = "error";
  private static final String ECS_EVENT = "event";
  private static final String ECS_EXCEPTION = "exception";
  private static final String ECS_EXTENSION = "extension";
  private static final String ECS_FAILURE = "failure";
  private static final String ECS_FRAGMENT = "fragment";
  private static final String ECS_FULL = "full";
  private static final String ECS_HASH = "hash";
  private static final String ECS_HOST = "host";
  private static final String ECS_HTTP = "http";
  private static final String ECS_ID = "id";
  private static final String ECS_INGESTED = "ingested";
  private static final String ECS_KIND = "kind";
  private static final String ECS_LABELS = "labels";
  private static final String ECS_LEVEL = "level";
  private static final String ECS_LOG = "log";
  private static final String ECS_MESSAGE = "message";
  private static final String ECS_METHOD = "method";
  private static final String ECS_MODULE = "module";
  private static final String ECS_NAME = "name";
  private static final String ECS_ORIGINAL = "original";
  private static final String ECS_OS = "os";
  private static final String ECS_OUTCOME = "outcome";

  @SuppressWarnings("squid:S2068") // Field name.
  private static final String ECS_PASSWORD = "password";

  private static final String ECS_PATH = "path";
  private static final String ECS_PORT = "port";
  private static final String ECS_PROVIDER = "provider";
  private static final String ECS_QUERY = "query";
  private static final String ECS_REFERRER = "referrer";
  private static final String ECS_REGISTERED_DOMAIN = "registered_domain";
  private static final String ECS_REQUEST = "request";
  private static final String ECS_RESPONSE = "response";
  private static final String ECS_RISK_SCORE = "risk_score";
  private static final String ECS_RISK_SCORE_NORM = "risk_score_norm";
  private static final String ECS_SCHEME = "scheme";
  private static final String ECS_SEQUENCE = "sequence";
  private static final String ECS_SERVICE = "service";
  private static final String ECS_SEVERITY = "severity";
  private static final String ECS_STACK_TRACE = "stack_trace";
  private static final String ECS_START = "start";
  private static final String ECS_STATUS_CODE = "status_code";
  private static final String ECS_SUCCESS = "success";
  private static final String ECS_TAGS = "tags";
  private static final String ECS_TEXT = "text";
  private static final String ECS_TIMESTAMP = "@timestamp";
  private static final String ECS_TIMEZONE = "timezone";
  private static final String ECS_TOP_LEVEL_DOMAIN = "top_level_domain";
  private static final String ECS_TRACE = "trace";
  private static final String ECS_TYPE = "type";
  private static final String ECS_URL = "url";
  private static final String ECS_USER = "user";
  private static final String ECS_USERNAME = "username";
  private static final String ECS_VERSION = "version";
  private static final String ECS_WEB = "web";
  private static final String UNKNOWN = "unknown";

  private String app;
  private String environment;
  private Level logLevel;
  private String service;
  private String serviceVersion;

  @SuppressWarnings("squid:S3398") // Not possible with static methods in non-static subclasses.
  private static JsonObjectBuilder content(final String s) {
    return createObjectBuilder().add(ECS_CONTENT, createObjectBuilder().add(ECS_TEXT, s));
  }

  @SuppressWarnings("squid:S3398") // Not possible with static methods in non-static subclasses.
  private static JsonObjectBuilder ecsOs() {
    return createObjectBuilder()
        .add(ECS_NAME, System.getProperty("os.name"))
        .add(ECS_VERSION, System.getProperty("os.version"));
  }

  @SuppressWarnings("squid:S3398") // Not possible with static methods in non-static subclasses.
  private static JsonObjectBuilder ecsVersion() {
    return createObjectBuilder().add(ECS_VERSION, "1.4.0");
  }

  /**
   * Checks if the given message is an ECS message.
   *
   * @param json the given JSON message.
   * @return The result is <code>true</code> when it is an ECS message.
   * @since 1.1
   */
  public static boolean isEcs(final JsonObject json) {
    return getValue(json, "/" + ECS + "/" + ECS_VERSION).isPresent();
  }

  @SuppressWarnings("squid:S3398") // Not possible with static methods in non-static subclasses.
  private static String skipFirst(final String s, final char first) {
    return !s.isEmpty() && s.charAt(0) == first ? s.substring(1) : s;
  }

  /**
   * Returns a new message builder.
   *
   * @return The builder.
   * @since 1.1
   */
  public Builder builder() {
    return new Builder();
  }

  /**
   * Returns the app property.
   *
   * @return The property.
   * @since 1.1
   */
  public String getApp() {
    return app;
  }

  /**
   * Returns the environment property.
   *
   * @return The property.
   * @since 1.1
   */
  public String getEnvironment() {
    return environment;
  }

  /**
   * Returns the log level property.
   *
   * @return The property.
   * @since 1.1
   */
  public Level getLogLevel() {
    return logLevel;
  }

  /**
   * Returns the service property.
   *
   * @return The property.
   * @since 1.1
   */
  public String getService() {
    return service;
  }

  /**
   * Returns the service version property.
   *
   * @return The property.
   * @since 1.1
   */
  public String getServiceVersion() {
    return serviceVersion;
  }

  /**
   * Sets the app property.
   *
   * @param app the property.
   * @return The object itself.
   * @since 1.1
   */
  public ElasticCommonSchema withApp(final String app) {
    this.app = app;

    return this;
  }

  /**
   * Sets the environment property.
   *
   * @param environment the property. It may be <code>null</code>.
   * @return The object itself.
   * @since 1.1
   */
  public ElasticCommonSchema withEnvironment(final String environment) {
    this.environment = environment;

    return this;
  }

  /**
   * Sets the default log level.
   *
   * @param logLevel the property.
   * @return The object itself.
   * @since 1.1
   */
  public ElasticCommonSchema withLogLevel(final Level logLevel) {
    this.logLevel = logLevel;

    return this;
  }

  /**
   * Sets the service property.
   *
   * @param service the property.
   * @return The object itself.
   * @since 1.1
   */
  public ElasticCommonSchema withService(final String service) {
    this.service = service;

    return this;
  }

  /**
   * Sets the service version property.
   *
   * @param serviceVersion the property.
   * @return The object itself.
   * @since 1.1
   */
  public ElasticCommonSchema withServiceVersion(final String serviceVersion) {
    this.serviceVersion = serviceVersion;

    return this;
  }

  /**
   * The message builder.
   *
   * @since 1.1
   */
  public class Builder {
    private final JsonObjectBuilder message = ecsGeneral();

    private Builder() {}

    public JsonObject build() {
      return message.build();
    }

    /**
     * Add any object as a field.
     *
     * @param field the field.
     * @param data the builder for the object.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder add(final String field, final JsonObjectBuilder data) {
      message.add(field, data);

      return this;
    }

    /**
     * Add a simple field.
     *
     * @param field the field.
     * @param value the value.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder add(final String field, final String value) {
      message.add(field, value);

      return this;
    }

    /**
     * Prepares the addition of an error object.
     *
     * @return A new error builder.
     * @since 1.1
     */
    public ErrorBuilder addError() {
      return new ErrorBuilder(this);
    }

    /**
     * Prepares the addition of an event object.
     *
     * @return A new event builder.
     * @since 1.1
     */
    public EventBuilder addEvent() {
      return new EventBuilder(this);
    }

    /**
     * Prepares the addition of an HTTP object.
     *
     * @return A new HTTP builder.
     * @since 1.1
     */
    public HttpBuilder addHttp() {
      return new HttpBuilder(this);
    }

    /**
     * Add something conditionally.
     *
     * @param test the test that should be passed.
     * @param add the function to add something.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder addIf(final Predicate<Builder> test, final UnaryOperator<Builder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    /**
     * Add something conditionally.
     *
     * @param value the function that produces a value.
     * @param add the function to add something.
     * @param <T> the value type.
     * @return The builder itself.
     * @since 1.2
     */
    public <T> Builder addIf(
        final Supplier<Optional<T>> value, final BiFunction<Builder, T, Builder> add) {
      return value.get().map(v -> add.apply(this, v)).orElse(this);
    }

    /**
     * Add the log level field.
     *
     * @param level the log level.
     * @return The builder itself.
     * @since 1.2.1
     */
    public Builder addLogLevel(final Level level) {
      return add(ECS_LOG, ecsLog(level));
    }

    /**
     * Add the message field.
     *
     * @param message the message itself.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder addMessage(final String message) {
      return add(ECS_MESSAGE, message);
    }

    /**
     * Add the timestamp field.
     *
     * @param timestamp the timestamp itself.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder addTimestamp(final Instant timestamp) {
      return add(ECS_TIMESTAMP, timestamp.toString());
    }

    /**
     * Add the trace field.
     *
     * @param id the trace ID.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder addTrace(final String id) {
      return add(ECS_TRACE, ecsTrace(id));
    }

    /**
     * Prepares the addition of a URL object.
     *
     * @return A new URL builder.
     * @since 1.2
     */
    public UrlBuilder addUrl() {
      return new UrlBuilder(this);
    }

    /**
     * Add the user field.
     *
     * @param username the username.
     * @return The builder itself.
     * @since 1.1
     */
    public Builder addUser(final String username) {
      return add(ECS_HOST, ecsHost(username));
    }

    private JsonObjectBuilder ecsGeneral() {
      return createObjectBuilder()
          .add(ECS_TIMESTAMP, now().toString())
          .add(ECS_SERVICE, ecsService())
          .add(ECS_AGENT, ecsService())
          .add(ECS_TAGS, ecsTags(environment != null ? new String[] {environment} : new String[0]))
          .add(ECS_LABELS, ecsLabels())
          .add(ECS_LOG, ecsLog(logLevel))
          .add(ECS, ecsVersion())
          .add(ECS_HOST, ecsHost(null));
    }

    private JsonObjectBuilder ecsHost(final String username) {
      return createObjectBuilder().add(ECS_OS, ecsOs()).add(ECS_USER, ecsUser(username));
    }

    private JsonObjectBuilder ecsLabels() {
      return createObjectBuilder()
          .add(ECS_APPLICATION, app != null ? app : UNKNOWN)
          .add(ECS_ENVIRONMENT, environment != null ? environment : UNKNOWN);
    }

    private JsonObjectBuilder ecsLog(final Level level) {
      return createObjectBuilder().add(ECS_LEVEL, level != null ? level.toString() : UNKNOWN);
    }

    private JsonObjectBuilder ecsService() {
      return createObjectBuilder()
          .add(ECS_NAME, service)
          .add(ECS_TYPE, "info")
          .add(ECS_VERSION, serviceVersion != null ? serviceVersion : UNKNOWN);
    }

    private JsonArrayBuilder ecsTags(final String[] tags) {
      return stream(tags)
          .filter(Objects::nonNull)
          .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
    }

    private JsonObjectBuilder ecsTrace(final String id) {
      return createObjectBuilder().add(ECS_ID, id);
    }

    private JsonObjectBuilder ecsUser(final String username) {
      return createObjectBuilder().add(ECS_NAME, username != null ? username : "anonymous");
    }
  }

  /**
   * The error message builder.
   *
   * @since 1.1
   */
  public class ErrorBuilder {
    private final Builder builder;
    private final JsonObjectBuilder error = createObjectBuilder();

    private ErrorBuilder(final Builder builder) {
      this.builder = builder;
    }

    /**
     * Add the error code.
     *
     * @param code the code.
     * @return The error builder itself.
     * @since 1.1
     */
    public ErrorBuilder addCode(final String code) {
      error.add(ECS_CODE, code);

      return this;
    }

    /**
     * Add the ID field.
     *
     * @param id the ID.
     * @return The error builder itself.
     * @since 1.1
     */
    public ErrorBuilder addId(final String id) {
      error.add(ECS_ID, id);

      return this;
    }

    /**
     * Add something conditionally.
     *
     * @param test the test that should be passed.
     * @param add the function to add something.
     * @return The error builder itself.
     * @since 1.1
     */
    public ErrorBuilder addIf(
        final Predicate<ErrorBuilder> test, final UnaryOperator<ErrorBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    /**
     * Add something conditionally.
     *
     * @param value the function that produces a value.
     * @param add the function to add something.
     * @param <T> the value type.
     * @return The error builder itself.
     * @since 1.2
     */
    public <T> ErrorBuilder addIf(
        final Supplier<Optional<T>> value, final BiFunction<ErrorBuilder, T, ErrorBuilder> add) {
      return value.get().map(v -> add.apply(this, v)).orElse(this);
    }

    /**
     * Add the error message.
     *
     * @param message the message.
     * @return The error builder itself.
     * @since 1.1
     */
    public ErrorBuilder addMessage(final String message) {
      error.add(ECS_MESSAGE, message);

      return this;
    }

    /**
     * Add an exception.
     *
     * @param t the exception.
     * @return The error builder itself.
     * @since 1.1
     */
    public ErrorBuilder addThrowable(final Throwable t) {
      error
          .add(ECS_MESSAGE, Optional.ofNullable(t.getMessage()).orElse(ECS_EXCEPTION))
          .add(ECS_STACK_TRACE, getStackTrace(t))
          .add(ECS_TYPE, t.getClass().getName());

      return this;
    }

    /**
     * Builds the error message and adds it to the general ECS builder.
     *
     * @return The ECS builder.
     * @since 1.1
     */
    public Builder build() {
      builder.message.add(ECS_ERROR, error);

      return builder;
    }
  }

  /**
   * The event message builder.
   *
   * @since 1.1
   */
  public class EventBuilder {
    private final Builder builder;
    private final JsonObjectBuilder event =
        createObjectBuilder()
            .add(ECS_CATEGORY, ECS_WEB)
            .add(ECS_CREATED, now().toString())
            .add(ECS_KIND, ECS_EVENT)
            .add(ECS_OUTCOME, ECS_SUCCESS)
            .add(ECS_TIMEZONE, systemDefault().toString());

    private EventBuilder(final Builder builder) {
      this.builder = builder;
    }

    /**
     * Add the action field.
     *
     * @param action the action.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addAction(final String action) {
      event.add(ECS_ACTION, action);

      return this;
    }

    /**
     * Add the code field.
     *
     * @param code the code.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addCode(final String code) {
      event.add(ECS_CODE, code);

      return this;
    }

    /**
     * Add the created field.
     *
     * @param created the creation time.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addCreated(final Instant created) {
      event.add(ECS_CREATED, created.toString());

      return this;
    }

    /**
     * Add the dataset field.
     *
     * @param dataset the dataset.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addDataset(final String dataset) {
      event.add(ECS_DATASET, dataset);

      return this;
    }

    /**
     * Add the duration field.
     *
     * @param duration the duration in milliseconds.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addDuration(final long duration) {
      event.add(ECS_DURATION, duration);

      return this;
    }

    /**
     * Add the end field.
     *
     * @param end the end time.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addEnd(final Instant end) {
      event.add(ECS_END, end.toString());

      return this;
    }

    /**
     * Set the outcome field to "failure".
     *
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addFailure() {
      event.add(ECS_OUTCOME, ECS_FAILURE);

      return this;
    }

    /**
     * Add the hash field.
     *
     * @param hash the hash.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addHash(final String hash) {
      event.add(ECS_HASH, hash);

      return this;
    }

    /**
     * Add the ID field.
     *
     * @param id the ID.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addId(final String id) {
      event.add(ECS_ID, id);

      return this;
    }

    /**
     * Add something conditionally.
     *
     * @param test the test that should be passed.
     * @param add the function to add something.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addIf(
        final Predicate<EventBuilder> test, final UnaryOperator<EventBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    /**
     * Add something conditionally.
     *
     * @param value the function that produces a value.
     * @param add the function to add something.
     * @param <T> the value type.
     * @return The event builder itself.
     * @since 1.2
     */
    public <T> EventBuilder addIf(
        final Supplier<Optional<T>> value, final BiFunction<EventBuilder, T, EventBuilder> add) {
      return value.get().map(v -> add.apply(this, v)).orElse(this);
    }

    /**
     * Add the ingested field.
     *
     * @param ingested the ingestion time.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addIngested(final Instant ingested) {
      event.add(ECS_INGESTED, ingested.toString());

      return this;
    }

    /**
     * Add the module field.
     *
     * @param module the module.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addModule(final String module) {
      event.add(ECS_MODULE, module);

      return this;
    }

    /**
     * Add the original field.
     *
     * @param original the original message.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addOriginal(final String original) {
      event.add(ECS_ORIGINAL, original);

      return this;
    }

    /**
     * Add the provider field.
     *
     * @param provider the provider.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addProvider(final String provider) {
      event.add(ECS_PROVIDER, provider);

      return this;
    }

    /**
     * Add the risk score field.
     *
     * @param riskScore the risk score.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addRiskScore(final float riskScore) {
      event.add(ECS_RISK_SCORE, riskScore);

      return this;
    }

    /**
     * Add the risk score norm field.
     *
     * @param riskScoreNorm the risk score norm.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addRiskScoreNorm(final float riskScoreNorm) {
      event.add(ECS_RISK_SCORE_NORM, riskScoreNorm);

      return this;
    }

    /**
     * Add the sequence field.
     *
     * @param sequence the sequence.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addSequence(final long sequence) {
      event.add(ECS_SEQUENCE, sequence);

      return this;
    }

    /**
     * Add the severity field.
     *
     * @param severity the severity.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addSeverity(final long severity) {
      event.add(ECS_SEVERITY, severity);

      return this;
    }

    /**
     * Add the start field.
     *
     * @param start the start time.
     * @return The event builder itself.
     * @since 1.1
     */
    public EventBuilder addStart(final Instant start) {
      event.add(ECS_START, start.toString());

      return this;
    }

    /**
     * Builds the event message and adds it to the general ECS builder.
     *
     * @return The ECS builder.
     * @since 1.1
     */
    public Builder build() {
      builder.message.add(ECS_EVENT, event);

      return builder;
    }
  }

  /**
   * The HTTP message builder.
   *
   * @since 1.1
   */
  public class HttpBuilder {
    private final Builder builder;
    private final JsonObjectBuilder http = createObjectBuilder().add(ECS_VERSION, "1.1");
    private final JsonObjectBuilder request = createObjectBuilder();
    private final JsonObjectBuilder requestBody = createObjectBuilder();
    private final JsonObjectBuilder response = createObjectBuilder();
    private final JsonObjectBuilder responseBody = createObjectBuilder();

    private HttpBuilder(final Builder builder) {
      this.builder = builder;
    }

    /**
     * Add something conditionally.
     *
     * @param test the test that should be passed.
     * @param add the function to add something.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addIf(
        final Predicate<HttpBuilder> test, final UnaryOperator<HttpBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    /**
     * Add something conditionally.
     *
     * @param value the function that produces a value.
     * @param add the function to add something.
     * @param <T> the value type.
     * @return The HTTP builder itself.
     * @since 1.2
     */
    public <T> HttpBuilder addIf(
        final Supplier<Optional<T>> value, final BiFunction<HttpBuilder, T, HttpBuilder> add) {
      return value.get().map(v -> add.apply(this, v)).orElse(this);
    }

    /**
     * Add the method field.
     *
     * @param method the method.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addMethod(final String method) {
      request.add(ECS_METHOD, method);

      return this;
    }

    /**
     * Add the referrer field.
     *
     * @param referrer the referrer.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addReferrer(final String referrer) {
      request.add(ECS_REFERRER, referrer);

      return this;
    }

    /**
     * Add the request body content field.
     *
     * @param content the content.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addRequestBodyContent(final String content) {
      requestBody.add(ECS_CONTENT, content(content));

      return this;
    }

    /**
     * Add the request body size field.
     *
     * @param size the body size.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addRequestBodySize(final long size) {
      requestBody.add(ECS_BYTES, size);

      return this;
    }

    /**
     * Add the request size field.
     *
     * @param size the request size.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addRequestSize(final long size) {
      request.add(ECS_BYTES, size);

      return this;
    }

    /**
     * Add the response body content field.
     *
     * @param content the content.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addResponseBodyContent(final String content) {
      responseBody.add(ECS_CONTENT, content(content));

      return this;
    }

    /**
     * Add the response body size field.
     *
     * @param size the body size.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addResponseBodySize(final long size) {
      responseBody.add(ECS_BYTES, size);

      return this;
    }

    /**
     * Add the response size field.
     *
     * @param size the response sizs.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addResponseSize(final long size) {
      response.add(ECS_BYTES, size);

      return this;
    }

    /**
     * Add the status code field.
     *
     * @param statusCode the status code.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addStatusCode(final long statusCode) {
      response.add(ECS_STATUS_CODE, statusCode);

      return this;
    }

    /**
     * Add the HTTP version field.
     *
     * @param version the HTTP version.
     * @return The HTTP builder itself.
     * @since 1.1
     */
    public HttpBuilder addVersion(final String version) {
      http.add(ECS_VERSION, version);

      return this;
    }

    /**
     * Builds the HTTP message and adds it to the general ECS builder.
     *
     * @return The ECS builder.
     * @since 1.1
     */
    public Builder build() {
      builder.message.add(
          ECS_HTTP,
          http.add(ECS_REQUEST, request.add(ECS_BODY, requestBody))
              .add(ECS_RESPONSE, response.add(ECS_BODY, responseBody)));

      return builder;
    }
  }

  /**
   * The URL message builder.
   *
   * @since 1.2
   */
  public class UrlBuilder {
    private final Builder builder;
    private final JsonObjectBuilder url = createObjectBuilder();

    private UrlBuilder(final Builder builder) {
      this.builder = builder;
    }

    /**
     * Add the domain.
     *
     * @param domain the domain.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addDomain(final String domain) {
      url.add(ECS_DOMAIN, domain);

      return this;
    }

    /**
     * Add the extension field.
     *
     * @param extension the extension.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addExtension(final String extension) {
      url.add(ECS_EXTENSION, skipFirst(extension, '.'));

      return this;
    }

    /**
     * Add the fragment field.
     *
     * @param fragment the fragment.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addFragment(final String fragment) {
      url.add(ECS_FRAGMENT, skipFirst(fragment, '#'));

      return this;
    }

    /**
     * Add the full URL field.
     *
     * @param fullUrl the full URL.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addFull(final String fullUrl) {
      url.add(ECS_FULL, fullUrl);

      return this;
    }

    /**
     * Add something conditionally.
     *
     * @param test the test that should be passed.
     * @param add the function to add something.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addIf(final Predicate<UrlBuilder> test, final UnaryOperator<UrlBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    /**
     * Add something conditionally.
     *
     * @param value the function that produces a value.
     * @param add the function to add something.
     * @param <T> the value type.
     * @return The URL builder itself.
     * @since 1.2
     */
    public <T> UrlBuilder addIf(
        final Supplier<Optional<T>> value, final BiFunction<UrlBuilder, T, UrlBuilder> add) {
      return value.get().map(v -> add.apply(this, v)).orElse(this);
    }

    /**
     * Add the original field.
     *
     * @param original the original URL.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addOriginal(final String original) {
      url.add(ECS_ORIGINAL, original);

      return this;
    }

    /**
     * Add the password field.
     *
     * @param password the password.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addPassword(final String password) {
      url.add(ECS_PASSWORD, password);

      return this;
    }

    /**
     * Add the path field.
     *
     * @param path the path.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addPath(final String path) {
      url.add(ECS_PATH, path);

      return this;
    }

    /**
     * Add the port field.
     *
     * @param port the port.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addPort(final int port) {
      url.add(ECS_PORT, port);

      return this;
    }

    /**
     * Add the query field.
     *
     * @param query the query.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addQuery(final String query) {
      url.add(ECS_QUERY, skipFirst(query, '?'));

      return this;
    }

    /**
     * Add the registered domain field.
     *
     * @param registeredDomain the registered domain.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addRegisteredDomain(final String registeredDomain) {
      url.add(ECS_REGISTERED_DOMAIN, skipFirst(registeredDomain, '.'));

      return this;
    }

    /**
     * Add the scheme field.
     *
     * @param scheme the scheme.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addScheme(final String scheme) {
      url.add(ECS_SCHEME, scheme);

      return this;
    }

    /**
     * Add the top level domain field.
     *
     * @param topLevelDomain the top level domain.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addTopLevelDomain(final String topLevelDomain) {
      url.add(ECS_TOP_LEVEL_DOMAIN, skipFirst(topLevelDomain, '.'));

      return this;
    }

    /**
     * Add the username field.
     *
     * @param username the username.
     * @return The URL builder itself.
     * @since 1.2
     */
    public UrlBuilder addUsername(final String username) {
      url.add(ECS_USERNAME, username);

      return this;
    }

    /**
     * Builds the URL message and adds it to the general ECS builder.
     *
     * @return The ECS builder.
     * @since 1.2
     */
    public Builder build() {
      builder.message.add(ECS_URL, url);

      return builder;
    }
  }
}

package net.pincette.jes.elastic;

import static java.time.Instant.now;
import static java.time.ZoneId.systemDefault;
import static java.util.Arrays.stream;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.util.Util.getStackTrace;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

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
  private static final String ECS_DURATION = "duration";
  private static final String ECS_END = "end";
  private static final String ECS_ENVIRONMENT = "environment";
  private static final String ECS_ERROR = "error";
  private static final String ECS_EVENT = "event";
  private static final String ECS_EXCEPTION = "exception";
  private static final String ECS_FAILURE = "failure";
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
  private static final String ECS_PROVIDER = "provider";
  private static final String ECS_REFERRER = "referrer";
  private static final String ECS_REQUEST = "request";
  private static final String ECS_RESPONSE = "response";
  private static final String ECS_RISK_SCORE = "risk_score";
  private static final String ECS_RISK_SCORE_NORM = "risk_score_norm";
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
  private static final String ECS_TRACE = "trace";
  private static final String ECS_TYPE = "type";
  private static final String ECS_USER = "user";
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

  public static boolean isEcs(final JsonObject json) {
    return getValue(json, "/" + ECS + "/" + ECS_VERSION).isPresent();
  }

  public Builder builder() {
    return new Builder();
  }

  public String getApp() {
    return app;
  }

  public String getEnvironment() {
    return environment;
  }

  public Level getLogLevel() {
    return logLevel;
  }

  public String getService() {
    return service;
  }

  public String getServiceVersion() {
    return serviceVersion;
  }

  public ElasticCommonSchema withApp(final String app) {
    this.app = app;

    return this;
  }

  public ElasticCommonSchema withEnvironment(final String environment) {
    this.environment = environment;

    return this;
  }

  public ElasticCommonSchema withLogLevel(final Level logLevel) {
    this.logLevel = logLevel;

    return this;
  }

  public ElasticCommonSchema withService(final String service) {
    this.service = service;

    return this;
  }

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
    private JsonObjectBuilder message = ecsGeneral();

    private Builder() {}

    public JsonObject build() {
      return message.build();
    }

    public Builder add(final String field, final JsonObjectBuilder data) {
      message.add(field, data);

      return this;
    }

    public Builder add(final String field, final String value) {
      message.add(field, value);

      return this;
    }

    public ErrorBuilder addError() {
      return new ErrorBuilder(this);
    }

    public EventBuilder addEvent() {
      return new EventBuilder(this);
    }

    public HttpBuilder addHttp() {
      return new HttpBuilder(this);
    }

    public Builder addIf(final Predicate<Builder> test, final UnaryOperator<Builder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    public Builder addMessage(final String message) {
      return add(ECS_MESSAGE, message);
    }

    public Builder addTimestamp(final Instant timestamp) {
      return add(ECS_TIMESTAMP, timestamp.toString());
    }

    public Builder addTrace(final String id) {
      return add(ECS_TRACE, ecsTrace(id));
    }

    public Builder addUser(final String username) {
      return add(ECS_HOST, ecsHost(username));
    }

    private JsonObjectBuilder ecsGeneral() {
      return createObjectBuilder()
          .add(ECS_TIMESTAMP, now().toString())
          .add(ECS_SERVICE, ecsService())
          .add(ECS_AGENT, ecsService())
          .add(ECS_TAGS, ecsTags(new String[] {environment}))
          .add(ECS_LABELS, ecsLabels())
          .add(ECS_LOG, ecsLog())
          .add(ECS, ecsVersion())
          .add(ECS_HOST, ecsHost(null));
    }

    private JsonObjectBuilder ecsHost(final String username) {
      return createObjectBuilder().add(ECS_OS, ecsOs()).add(ECS_USER, ecsUser(username));
    }

    private JsonObjectBuilder ecsLabels() {
      return createObjectBuilder().add(ECS_APPLICATION, app).add(ECS_ENVIRONMENT, environment);
    }

    private JsonObjectBuilder ecsLog() {
      return createObjectBuilder().add(ECS_LEVEL, logLevel.toString());
    }

    private JsonObjectBuilder ecsService() {
      return createObjectBuilder()
          .add(ECS_NAME, service)
          .add(ECS_TYPE, "info")
          .add(ECS_VERSION, serviceVersion != null ? serviceVersion : UNKNOWN);
    }

    private JsonArrayBuilder ecsTags(final String[] tags) {
      return stream(tags).reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
    }

    private JsonObjectBuilder ecsTrace(final String id) {
      return createObjectBuilder().add(ECS_ID, id);
    }

    private JsonObjectBuilder ecsUser(final String username) {
      return createObjectBuilder().add(ECS_NAME, username != null ? username : "anonymous");
    }
  }

  public class ErrorBuilder {
    private Builder builder;
    private JsonObjectBuilder error = createObjectBuilder();

    private ErrorBuilder(final Builder builder) {
      this.builder = builder;
    }

    public ErrorBuilder addCode(final String code) {
      error.add(ECS_CODE, code);

      return this;
    }

    public ErrorBuilder addId(final String id) {
      error.add(ECS_ID, id);

      return this;
    }

    public ErrorBuilder addIf(
        final Predicate<ErrorBuilder> test, final UnaryOperator<ErrorBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    public ErrorBuilder addMessage(final String message) {
      error.add(ECS_MESSAGE, message);

      return this;
    }

    public ErrorBuilder addThrowable(final Throwable t) {
      error
          .add(ECS_MESSAGE, Optional.ofNullable(t.getMessage()).orElse(ECS_EXCEPTION))
          .add(ECS_STACK_TRACE, getStackTrace(t))
          .add(ECS_TYPE, t.getClass().getName());

      return this;
    }

    public Builder build() {
      builder.message.add(ECS_ERROR, error);

      return builder;
    }
  }

  public class EventBuilder {
    private Builder builder;
    private JsonObjectBuilder event =
        createObjectBuilder()
            .add(ECS_CATEGORY, ECS_WEB)
            .add(ECS_CREATED, now().toString())
            .add(ECS_KIND, ECS_EVENT)
            .add(ECS_OUTCOME, ECS_SUCCESS)
            .add(ECS_TIMEZONE, systemDefault().toString());

    private EventBuilder(final Builder builder) {
      this.builder = builder;
    }

    public EventBuilder addAction(final String action) {
      event.add(ECS_ACTION, action);

      return this;
    }

    public EventBuilder addCode(final String code) {
      event.add(ECS_CODE, code);

      return this;
    }

    public EventBuilder addCreated(final Instant created) {
      event.add(ECS_CREATED, created.toString());

      return this;
    }

    public EventBuilder addDataset(final String dataset) {
      event.add(ECS_DATASET, dataset);

      return this;
    }

    public EventBuilder addDuration(final long duration) {
      event.add(ECS_DURATION, duration);

      return this;
    }

    public EventBuilder addEnd(final Instant end) {
      event.add(ECS_END, end.toString());

      return this;
    }

    public EventBuilder addFailure() {
      event.add(ECS_OUTCOME, ECS_FAILURE);

      return this;
    }

    public EventBuilder addHash(final String hash) {
      event.add(ECS_HASH, hash);

      return this;
    }

    public EventBuilder addId(final String id) {
      event.add(ECS_ID, id);

      return this;
    }

    public EventBuilder addIf(
        final Predicate<EventBuilder> test, final UnaryOperator<EventBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    public EventBuilder addIngested(final Instant ingested) {
      event.add(ECS_INGESTED, ingested.toString());

      return this;
    }

    public EventBuilder addModule(final String module) {
      event.add(ECS_MODULE, module);

      return this;
    }

    public EventBuilder addOriginal(final String original) {
      event.add(ECS_ORIGINAL, original);

      return this;
    }

    public EventBuilder addProvider(final String provider) {
      event.add(ECS_PROVIDER, provider);

      return this;
    }

    public EventBuilder addRiskScore(final float riskScore) {
      event.add(ECS_RISK_SCORE, riskScore);

      return this;
    }

    public EventBuilder addRiskScoreNorm(final float riskScoreNorm) {
      event.add(ECS_RISK_SCORE_NORM, riskScoreNorm);

      return this;
    }

    public EventBuilder addSequence(final long sequence) {
      event.add(ECS_SEQUENCE, sequence);

      return this;
    }

    public EventBuilder addSeverity(final long severity) {
      event.add(ECS_SEVERITY, severity);

      return this;
    }

    public EventBuilder addStart(final Instant start) {
      event.add(ECS_START, start.toString());

      return this;
    }

    public Builder build() {
      builder.message.add(ECS_EVENT, event);

      return builder;
    }
  }

  public class HttpBuilder {
    private Builder builder;
    private JsonObjectBuilder http = createObjectBuilder().add(ECS_VERSION, "1.1");
    private JsonObjectBuilder request = createObjectBuilder();
    private JsonObjectBuilder requestBody = createObjectBuilder();
    private JsonObjectBuilder response = createObjectBuilder();
    private JsonObjectBuilder responseBody = createObjectBuilder();

    private HttpBuilder(final Builder builder) {
      this.builder = builder;
    }

    public HttpBuilder addIf(
        final Predicate<HttpBuilder> test, final UnaryOperator<HttpBuilder> add) {
      return test.test(this) ? add.apply(this) : this;
    }

    public HttpBuilder addMethod(final String method) {
      request.add(ECS_METHOD, method);

      return this;
    }

    public HttpBuilder addReferrer(final String referrer) {
      request.add(ECS_REFERRER, referrer);

      return this;
    }

    public HttpBuilder addRequestBodyContent(final String content) {
      requestBody.add(ECS_CONTENT, content(content));

      return this;
    }

    public HttpBuilder addRequestBodySize(final long size) {
      requestBody.add(ECS_BYTES, size);

      return this;
    }

    public HttpBuilder addRequestSize(final long size) {
      request.add(ECS_BYTES, size);

      return this;
    }

    public HttpBuilder addResponseBodyContent(final String content) {
      responseBody.add(ECS_CONTENT, content(content));

      return this;
    }

    public HttpBuilder addResponseBodySize(final long size) {
      responseBody.add(ECS_BYTES, size);

      return this;
    }

    public HttpBuilder addResponseSize(final long size) {
      response.add(ECS_BYTES, size);

      return this;
    }

    public HttpBuilder addStatusCode(final long statusCode) {
      response.add(ECS_STATUS_CODE, statusCode);

      return this;
    }

    public HttpBuilder addVersion(final String version) {
      http.add(ECS_VERSION, version);

      return this;
    }

    public Builder build() {
      builder.message.add(
          ECS_HTTP,
          http.add(ECS_REQUEST, request.add(ECS_BODY, requestBody))
              .add(ECS_RESPONSE, response.add(ECS_BODY, responseBody)));

      return builder;
    }
  }
}

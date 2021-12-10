package net.pincette.jes.elastic;

import static net.pincette.jes.elastic.Util.sendMessage;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Util.tryToGetRethrow;

import javax.json.JsonObject;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Connects JSON Event Sourcing aggregates to an Elasticsearch index.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Aggregates {
  private Aggregates() {}

  /**
   * Consumes the <code>aggregates</code> stream and forwards all JSON documents to an Elasticsearch
   * index. The name of the index is composed of the <code>_type</code> field of the JSON document,
   * followed by the suffix "-<code>environment</code>".
   *
   * @param aggregates the aggregate stream.
   * @param environment the environment name, e.g. "tst", "acc", "dev". It may be <code>null</code>.
   * @param uri the Elasticsearch endpoint.
   * @param authorizationHeader the value for the Authorization header on each request.
   * @since 1.0
   */
  public static void connect(
      final KStream<String, JsonObject> aggregates,
      final String environment,
      final String uri,
      final String authorizationHeader) {
    aggregates.mapValues(
        v ->
            tryToGetRethrow(
                    () ->
                        sendMessage(
                                string(removeMetadata(v)),
                                createUri(uri, v, environment),
                                "PUT",
                                "application/json",
                                authorizationHeader)
                            .toCompletableFuture()
                            .get())
                .orElse(false));
  }

  private static String createUri(
      final String uri, final JsonObject aggregate, final String environment) {
    return uri
        + (uri.endsWith("/") ? "" : "/")
        + aggregate.getString(TYPE)
        + (environment != null ? ("-" + environment) : "")
        + "/_doc/"
        + aggregate.getString(ID);
  }

  private static JsonObject removeMetadata(final JsonObject aggregate) {
    return createObjectBuilder(aggregate).remove(ID).remove(TYPE).remove(TIMESTAMP).build();
  }
}

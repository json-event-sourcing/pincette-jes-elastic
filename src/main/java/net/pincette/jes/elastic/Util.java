package net.pincette.jes.elastic;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.util.Or.tryWith;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import org.asynchttpclient.AsyncHttpClient;

class Util {
  private static final AsyncHttpClient client = asyncHttpClient();

  private Util() {}

  static String errorMessage(final JsonObject command) {
    return tryWith(() -> command.getString("message", null))
        .or(() -> command.getString("exception", null))
        .or(() -> "Validation error")
        .get()
        .orElse("Unknown error");
  }

  static CompletionStage<Boolean> sendMessage(
      final String message,
      final String uri,
      final String method,
      final String type,
      final String authorizationHeader) {
    return Optional.ofNullable(message)
        .map(
            m ->
                client
                    .prepare(method, uri)
                    .setHeader("Authorization", authorizationHeader)
                    .setHeader("Content-Type", type)
                    .setBody(m)
                    .execute()
                    .toCompletableFuture()
                    .thenApply(r -> r.getStatusCode() == 202))
        .orElse(completedFuture(false));
  }
}

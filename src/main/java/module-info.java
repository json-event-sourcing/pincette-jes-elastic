module net.pincette.jes.elastic {
  requires net.pincette.common;
  requires async.http.client;
  requires java.json;
  requires net.pincette.jes.util;
  requires net.pincette.json;
  requires kafka.streams;
  requires net.pincette.jes;
  requires java.logging;
  requires kafka.clients;
  exports net.pincette.jes.elastic;
}

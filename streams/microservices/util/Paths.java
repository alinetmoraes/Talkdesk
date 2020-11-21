package io.confluent.examples.streams.microservices.util;

public class Paths {

  private final String base;

  public Paths(final String host, final int port) {
    base = "http://" + host + ":" + port;
  }


  public String urlGet(final String id) {
    return base + "/v1/calls/" + id;
  }


  public String urlPost() {
    return base + "/v1/calls/";
  }
}

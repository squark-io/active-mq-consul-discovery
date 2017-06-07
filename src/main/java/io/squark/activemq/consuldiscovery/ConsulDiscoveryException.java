package io.squark.activemq.consuldiscovery;

public class ConsulDiscoveryException extends RuntimeException {
  public ConsulDiscoveryException(String message) {
    super(message);
  }

  public ConsulDiscoveryException(Throwable cause) {
    super(cause);
  }
}

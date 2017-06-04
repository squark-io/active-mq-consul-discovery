package io.squark.activemq.consuldiscovery;

import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class ConsulDiscoveryAgentFactory extends DiscoveryAgentFactory {

  private final static Logger LOG = LoggerFactory.getLogger(ConsulDiscoveryAgentFactory.class);

  @Override
  protected DiscoveryAgent doCreateDiscoveryAgent(URI consulURI) throws IOException {
    try {
      ConsulDiscoveryAgent agent = new ConsulDiscoveryAgent(consulURI);
      LOG.info("Consul DiscoveryAgent successfully created.");
      return agent;
    } catch (Throwable e) {
      LOG.error("Failed to create Consul DiscoveryAgent for URI " + consulURI, e);
      throw new IOException("Failed to create Consul DiscoveryAgent for URI " + consulURI, e);
    }
  }
}

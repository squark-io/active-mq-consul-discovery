package io.squark.activemq.consuldiscovery;

import com.orbitz.consul.Consul;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegCheck;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Based on {@link org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent} but will perform discovery
 * against Consul every 30 seconds and update available brokers accordingly.
 * <p>
 * The DiscoveryAgent will also register itself with Consul and perform a TTL check at the time of service lookup.
 * <p>
 * When the JVM is properly terminated, a shutdown hook will also deregister the broker from Consul.
 */
public class ConsulDiscoveryAgent implements DiscoveryAgent {

  private final static Logger LOG = LoggerFactory.getLogger(ConsulDiscoveryAgent.class);
  static long pollingInterval = 30000L;
  private static int BACKOFF_MULTIPLIER = 2;
  private static long maxQuarantineTime = 120000;
  private static ConsulHolder CONSUL_HOLDER;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final URI basePath;
  private final String consulServiceName;
  private final String consulServiceId;
  private final String consulServiceAddress;
  private final String consulServicePort;
  private final Map<String, ConsulDiscoveryEvent> quarantinedServices = new HashMap<String, ConsulDiscoveryEvent>();
  private boolean clientOnly = false;
  private Map<String, Service> services = new HashMap<String, Service>();
  private DiscoveryListener listener;
  private TaskRunnerFactory taskRunner;
  private ScheduledExecutorService executorService;
  private final Thread shutdownHook = new Thread() {
    @Override
    public void run() {
      LOG.info("Shutdown hook executed. Deregistering from Consul");
      if (CONSUL_HOLDER != null && CONSUL_HOLDER.hasInstance()) {
        CONSUL_HOLDER.deregister(consulServiceId);
      }
      try {
        if (executorService != null) {
          executorService.shutdown();
        }
      } catch (Exception e) {
        throw new ConsulDiscoveryException(e);
      }
    }
  };

  public ConsulDiscoveryAgent(URI consulURI) {
    try {
      LOG.info("Creating Consul Discovery Agent for URI " + consulURI);
      URISupport.CompositeData data = URISupport.parseComposite(consulURI);
      Map options = data.getParameters();
      IntrospectionSupport.setProperties(this, options);
      if (data.getComponents().length != 1) {
        throw new ConsulDiscoveryException(
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService&amp;address=amq.example" +
          ".com&amp;port=61616)\". Found " + consulURI);
      }
      URI baseURI = data.getComponents()[0];
      LOG.info("Consul base URI: " + baseURI);
      URI basePath = URISupport.removeQuery(baseURI);
      Map<String, String> parameters = URISupport.parseParameters(baseURI);
      boolean clientOnly = false;
      if (parameters.containsKey("clientOnly")) {
        clientOnly = Boolean.parseBoolean(parameters.get("clientOnly"));
      }
      if (!parameters.containsKey("service") ||
          !clientOnly && (!parameters.containsKey("address") || !parameters.containsKey("port"))) {
        throw new ConsulDiscoveryException(
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService&amp;address=amq.example" +
          ".com&amp;port=61616)\". Found " + consulURI);
      }
      this.clientOnly = clientOnly;
      if (!basePath.toString().endsWith("/")) {
        basePath = basePath.resolve("/");
      }
      this.basePath = basePath;
      consulServiceName = parameters.get("service");
      consulServiceId = parameters.get("serviceId") != null ? parameters.get("serviceId") :
                        Long.toHexString(UUID.randomUUID().getLeastSignificantBits());
      consulServiceAddress = parameters.get("address");
      consulServicePort = parameters.get("port");

      String maxQuarantineTimeString = parameters.get("maxQuarantineTime");
      if (maxQuarantineTimeString != null) {
        try {
          maxQuarantineTime = Long.parseLong(maxQuarantineTimeString);
        } catch (NumberFormatException e) {
          throw new ConsulDiscoveryException("Failed to parse maxQuarantineTime \"" + maxQuarantineTimeString + "\"", e);
        }
      }
      String intervalString = parameters.get("interval");
      if (intervalString != null) {
        try {
          pollingInterval = Long.parseLong(intervalString);
        } catch (NumberFormatException e) {
          throw new ConsulDiscoveryException("Failed to parse interval \"" + intervalString + "\"", e);
        }
      }
    } catch (Throwable e) {
      LOG.error("Failed to create Consul DiscoveryAgent for URI " + consulURI, e);
      if (e instanceof ConsulDiscoveryException) {
        throw (ConsulDiscoveryException) e;
      }
      throw new ConsulDiscoveryException(e);
    }
  }

  @Override
  public void setDiscoveryListener(DiscoveryListener listener) {
    this.listener = listener;
  }

  @Override
  public void registerService(String name) throws IOException {
  }

  @Override
  public void serviceFailed(DiscoveryEvent discoveryEvent) throws IOException {
    final ConsulDiscoveryEvent consulDiscoveryEvent = (ConsulDiscoveryEvent) discoveryEvent;
    if (running.get()) {
      // Simply remove the failing service and let service discovery handle reconnection.
      services.remove(consulDiscoveryEvent.getService().getId());
      listener.onServiceRemove(consulDiscoveryEvent);
      long delay = Math.max(pollingInterval, BACKOFF_MULTIPLIER * consulDiscoveryEvent.numberOfFails * pollingInterval);
      if (delay > maxQuarantineTime) {
        delay = maxQuarantineTime;
      }
      consulDiscoveryEvent.numberOfFails++;
      if (consulDiscoveryEvent.failAt <= 0) {
        consulDiscoveryEvent.failAt = System.currentTimeMillis();
      } else {
        if ((System.currentTimeMillis() - consulDiscoveryEvent.connectAt) > 60000L) {
          LOG.warn("Quarantined service " + consulDiscoveryEvent.service.getId() +
                   " has previous failed connection attempts, but the last attempt started over 1 minute ago. This suggests " +
                   "that we're experiencing a new, unrelated, error event. Resetting quarantine.");
          consulDiscoveryEvent.failAt = System.currentTimeMillis();
          consulDiscoveryEvent.numberOfFails = 1;
          delay = Math.max(pollingInterval, BACKOFF_MULTIPLIER * consulDiscoveryEvent.numberOfFails * pollingInterval);
          if (delay > maxQuarantineTime) {
            delay = maxQuarantineTime;
          }
        }
      }
      consulDiscoveryEvent.quarantineUntil = consulDiscoveryEvent.failAt + delay;
      String message = String.format("%02d minute(s) and %02d second(s)", TimeUnit.MILLISECONDS.toMinutes(delay),
        (TimeUnit.MILLISECONDS.toSeconds(delay) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(delay))));
      LOG.warn("Failed service connection to " + consulDiscoveryEvent.service.getId() + ". Quarantining for " + message);
      quarantinedServices.put(consulDiscoveryEvent.service.getId(), consulDiscoveryEvent);
    }
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting Consul Discovery Agent for " + basePath);
    taskRunner = new TaskRunnerFactory("Consul Discovery Agent");
    taskRunner.init();
    running.set(true);

    if (CONSUL_HOLDER == null || !CONSUL_HOLDER.hasInstance()) {
      CONSUL_HOLDER = ConsulHolder.initialize(Consul.builder().withUrl(basePath.toURL()).build(), this);
    } else {
      CONSUL_HOLDER = ConsulHolder.instance();
    }
    registerSelf();

    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (running.get()) {
          try {
            if (CONSUL_HOLDER == null || !CONSUL_HOLDER.hasInstance()) {
              CONSUL_HOLDER =
                ConsulHolder.initialize(Consul.builder().withUrl(basePath.toURL()).build(), ConsulDiscoveryAgent.this);
            }
            checkTtl();

            List<ServiceHealth> services = CONSUL_HOLDER.getServices(consulServiceName);
            handleServices(services);
          } catch (Throwable t) {
            //We don't really want this thread to ever die as long as the agent is running, so we'll just log any
            // errors.
            LOG.warn("Failure in Consul service discovery", t);
          }
        } else {
          Thread.currentThread().interrupt();
        }
      }
    }, 0, pollingInterval, TimeUnit.MILLISECONDS);
  }

  private void checkTtl() throws NotRegisteredException {
    if (!clientOnly) {
      CONSUL_HOLDER.checkTtl(consulServiceId);
    }
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Consul Discovery Agent for " + basePath);
    running.set(false);

    if (taskRunner != null) {
      taskRunner.shutdown();
    }

    for (Map.Entry<String, Service> service : services.entrySet()) {
      listener.onServiceRemove(new ConsulDiscoveryEvent(service.getValue()));
    }
    services.clear();

    if (executorService != null) {
      executorService.shutdown();
    }
  }

  private void handleServices(Collection<ServiceHealth> serviceList) throws MalformedURLException {
    LOG.debug("Found (" + serviceList.size() + ") services.");
    List<ServiceHealth> servicesToAdd = new ArrayList<ServiceHealth>();
    List<ServiceHealth> servicesToDelete = new ArrayList<ServiceHealth>();
    boolean foundSelf = false;
    for (ServiceHealth serviceNode : serviceList) {
      boolean passing = true;
      if (serviceNode.getService().getId().equals(consulServiceId)) {
        foundSelf = true;
        // Found self, not adding.
        continue;
      } else {
        for (HealthCheck check : serviceNode.getChecks()) {
          if (!check.getStatus().equals("passing")) {
            passing = false;
            break;
          }
        }
      }
      if (passing) {
        servicesToAdd.add(serviceNode);
      } else {
        servicesToDelete.add(serviceNode);
      }
    }
    for (ServiceHealth serviceNode : servicesToDelete) {
      if (services.containsKey(serviceNode.getService().getId())) {
        LOG.info("Removing service " + serviceNode.getService().getId() + " with failing checks");
        services.remove(serviceNode.getService().getId());
        listener.onServiceRemove(new ConsulDiscoveryEvent(serviceNode.getService()));
      }
    }
    List<String> servicesAdded = new ArrayList<String>();
    for (ServiceHealth serviceNode : servicesToAdd) {
      ConsulDiscoveryEvent quarantinedEvent = quarantinedServices.get(serviceNode.getService().getId());
      if (quarantinedEvent != null) {
        if (quarantinedEvent.quarantineUntil > System.currentTimeMillis()) {
          long delay = quarantinedEvent.quarantineUntil - System.currentTimeMillis();
          String message = String.format("%02d minute(s) and %02d second(s)", TimeUnit.MILLISECONDS.toMinutes(delay),
            (TimeUnit.MILLISECONDS.toSeconds(delay) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(delay))));
          LOG.warn(
            "Service " + serviceNode.getService().getId() + " was found but is quarantined another " + message + " due to " +
            quarantinedEvent.numberOfFails + " previous failure(s)");
          continue;
        } else {
          LOG.warn("Service " + serviceNode.getService().getId() + " was quarantined but is now allowed to attempt again");
        }
      }
      if (!services.containsKey(serviceNode.getService().getId())) {
        // Only logging if not already added.
        LOG.info("Adding service " + serviceNode.getService().getId());
      }
      services.put(serviceNode.getService().getId(), serviceNode.getService());
      ConsulDiscoveryEvent event;
      if (quarantinedEvent != null) {
        event = quarantinedEvent;
        //Update service in case something changed.
        event.service = serviceNode.getService();
      } else {
        event = new ConsulDiscoveryEvent(serviceNode.getService());
      }
      event.connectAt = System.currentTimeMillis();
      //Remove event from quarantine as it will be added again anyway in case it fails, AND we don't want it to
      // remain in quarantine in case it succeeds.
      quarantinedServices.remove(event.service.getId());
      listener.onServiceAdd(event);
      servicesAdded.add(serviceNode.getService().getId());
    }
    List<String> orphanedServices = new ArrayList<String>(services.keySet());
    orphanedServices.removeAll(servicesAdded);
    for (String toDelete : orphanedServices) {
      listener.onServiceRemove(new ConsulDiscoveryEvent(services.get(toDelete)));
      services.remove(toDelete);
      LOG.info("Removing no longer available service " + toDelete);
    }
    if (!foundSelf) {
      // We did not find ourselves. We were de-registered somehow. Re-registering.
      registerSelf();
    }
  }

  private void registerSelf() throws MalformedURLException {
    if (clientOnly) {
      LOG.debug("Client only. Not registering self with consul");
      return;
    }
    LOG.info("Registering self with Consul");
    Registration.RegCheck ttlCheck = ImmutableRegCheck.builder().ttl("60s").deregisterCriticalServiceAfter("1m").build();
    Registration registration =
      ImmutableRegistration.builder().address(consulServiceAddress).port(Integer.parseInt(consulServicePort))
        .name(consulServiceName).id(consulServiceId).check(ttlCheck).build();
    if (CONSUL_HOLDER != null && CONSUL_HOLDER.hasInstance()) {
      CONSUL_HOLDER.registerSelf(registration);
    } else {
      CONSUL_HOLDER = ConsulHolder.initialize(Consul.builder().withUrl(basePath.toURL()).build(), this);
      CONSUL_HOLDER.registerSelf(registration);
    }
    Runtime.getRuntime().removeShutdownHook(shutdownHook);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  class ConsulDiscoveryEvent extends DiscoveryEvent {

    private final AtomicBoolean failed = new AtomicBoolean(false);
    public int numberOfFails;
    public long failAt = -1;
    public long quarantineUntil;
    public long connectAt;
    private Service service;

    public ConsulDiscoveryEvent(Service service) {
      super("tcp://" + service.getAddress() + ":" + service.getPort());
      this.service = service;
    }

    public Service getService() {
      return service;
    }

    @Override
    public String toString() {
      return "ConsulDiscoveryEvent{" + "failed=" + failed + ", service=" + service + ", serviceName='" + serviceName + '\'' +
             ", brokerName='" + brokerName + '\'' + '}';
    }
  }
}

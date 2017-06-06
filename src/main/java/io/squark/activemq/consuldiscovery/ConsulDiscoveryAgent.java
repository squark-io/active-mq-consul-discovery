package io.squark.activemq.consuldiscovery;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.agent.ImmutableRegCheck;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.QueryOptions;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsulDiscoveryAgent implements DiscoveryAgent {

  private final static Logger LOG = LoggerFactory.getLogger(ConsulDiscoveryAgent.class);
  private final Object sleepMutex = new Object();
  private final Object pollSleepMutex = new Object();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final URI basePath;
  private final String consulServiceName;
  private final String consulServiceId;
  private final String consulServiceAddress;
  private final String consulServicePort;
  private long initialReconnectDelay = 1000;
  private long maxReconnectDelay = 1000 * 30;
  private long backOffMultiplier = 2;
  private boolean useExponentialBackOff = true;
  private int maxReconnectAttempts;
  private long minConnectTime = 5000;
  private DiscoveryListener listener;
  private Set<Service> services = new HashSet<Service>();
  private TaskRunnerFactory taskRunner;

  private Consul consul;
  private ServiceHealthCache serviceHealthCache;
  private ConsulCache.Listener<ServiceHealthKey, ServiceHealth> serviceListener = new ConsulCache
    .Listener<ServiceHealthKey, ServiceHealth>() {
    @Override
    public void notify(Map<ServiceHealthKey, ServiceHealth> newValues) {
      handleServices(newValues.values());
    }
  };
  private final Thread shutdownHook = new Thread() {
    @Override
    public void run() {
      LOG.info("Shutdown hook executed. Deregistering from Consul");
      try {
        serviceHealthCache.removeListener(serviceListener);
        serviceHealthCache.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      consul.agentClient().deregister(consulServiceId);
    }
  };

  public ConsulDiscoveryAgent(URI consulURI) {
    try {
      LOG.info("Starting Consul discovery for URI " + consulURI);
      URISupport.CompositeData data = URISupport.parseComposite(consulURI);
      Map options = data.getParameters();
      IntrospectionSupport.setProperties(this, options);
      if (data.getComponents().length != 1) {
        throw new IOException(
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService&amp;address=amq.example.com&amp;port=61616)?params\". Found " +
            consulURI);
      }
      URI baseURI = data.getComponents()[0];
      LOG.info("Consul base URI: " + baseURI);
      URI basePath = URISupport.removeQuery(baseURI);
      Map<String, String> parameters = URISupport.parseParameters(baseURI);
      if (!parameters.containsKey("service") || !parameters.containsKey("address") || !parameters.containsKey("port")) {
        throw new IOException(
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService&amp;address=amq.example.com&amp;port=61616)?params\". Found " +
            consulURI);
      }
      if (!basePath.toString().endsWith("/")) {
        basePath = basePath.resolve("/");
      }
      this.basePath = basePath;
      consulServiceName = parameters.get("service");
      consulServiceId = parameters.get("serviceId") != null ? parameters.get("serviceId") : Long
        .toHexString(UUID.randomUUID().getLeastSignificantBits());
      consulServiceAddress = parameters.get("address");
      consulServicePort = parameters.get("port");
    } catch (Throwable e) {
      LOG.error("Failed to create Consul DiscoveryAgent for URI " + consulURI, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
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
  public void start() throws Exception {
    LOG.info("Starting Consul Discovery Agent for " + basePath);
    taskRunner = new TaskRunnerFactory("Consul Discovery Agent");
    taskRunner.init();
    running.set(true);

    consul = Consul.builder().withUrl(basePath.toURL()).build();
    registerSelf();

    try {
      HealthClient healthClient = consul.healthClient();
      List<ServiceHealth> services = healthClient.getAllServiceInstances(consulServiceName).getResponse();
      handleServices(services);
      serviceHealthCache = ServiceHealthCache
        .newCache(healthClient, consulServiceName, false, QueryOptions.BLANK, 30);
      serviceHealthCache.addListener(serviceListener);

      serviceHealthCache.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void handleServices(Collection<ServiceHealth> serviceList) {
    LOG.info("Found (" + serviceList.size() + ") services.");
    List<ServiceHealth> servicesToAdd = new ArrayList<ServiceHealth>();
    List<ServiceHealth> servicesToDelete = new ArrayList<ServiceHealth>();
    boolean foundSelf = false;
    for (ServiceHealth serviceNode : serviceList) {
      boolean passing = true;
      if (serviceNode.getService().getId().equals(consulServiceId)) {
        foundSelf = true;
        //Found self, not adding.
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
      if (services.contains(serviceNode.getService())) {
        LOG.info("Removing service " + serviceNode.getService().getId() + " with failing checks");
        services.remove(serviceNode.getService());
        listener.onServiceRemove(new ConsulDiscoveryEvent(serviceNode.getService()));
      }
    }
    List<Service> servicesAdded = new ArrayList<Service>();
    for (ServiceHealth serviceNode : servicesToAdd) {
      if (!services.contains(serviceNode.getService())) {
        //Only logging if not already added.
        LOG.info("Adding service " + serviceNode.getService().getId());
      }
      services.add(serviceNode.getService());
      listener.onServiceAdd(new ConsulDiscoveryEvent(serviceNode.getService()));
      servicesAdded.add(serviceNode.getService());
    }
    List<Service> orphanedServices = new ArrayList<Service>(services);
    orphanedServices.removeAll(servicesAdded);
    for (Service toDelete : orphanedServices) {
      listener.onServiceRemove(new ConsulDiscoveryEvent(toDelete));
      LOG.info("Removing no longer available service " + toDelete);
    }
    if (!foundSelf) {
      //We did not find ourselves. We were deregistered somehow. Registering.
      registerSelf();
    }

  }

  private void registerSelf() {
    LOG.info("Registering self with Consul");
    Registration.RegCheck tcpCheck = ImmutableRegCheck.builder()
      .tcp(consulServiceAddress + ":" + consulServicePort)
      .interval("10s")
      .timeout("10s")
      .deregisterCriticalServiceAfter("1m").build();
    Registration registration = ImmutableRegistration.builder()
      .address(consulServiceAddress)
      .port(Integer.parseInt(consulServicePort))
      .name(consulServiceName)
      .id(consulServiceId)
      .check(tcpCheck)
      .build();
    consul.agentClient().register(registration);
    Runtime.getRuntime().removeShutdownHook(shutdownHook);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Consul Discovery Agent for " + basePath);
    running.set(false);

    if (taskRunner != null) {
      taskRunner.shutdown();
    }

    for (Service service : services) {
      listener.onServiceRemove(new ConsulDiscoveryEvent(service));
    }

    synchronized (sleepMutex) {
      sleepMutex.notifyAll();
    }
    synchronized (pollSleepMutex) {
      pollSleepMutex.notifyAll();
    }
  }

  @Override
  public void serviceFailed(DiscoveryEvent devent) throws IOException {

    final ConsulDiscoveryEvent consulDiscoveryEvent = (ConsulDiscoveryEvent) devent;
    if (running.get() && consulDiscoveryEvent.failed.compareAndSet(false, true)) {

      services.remove(consulDiscoveryEvent.getService());
      listener.onServiceRemove(consulDiscoveryEvent);

      taskRunner.execute(new Runnable() {
        @Override
        public void run() {
          ConsulDiscoveryEvent event = new ConsulDiscoveryEvent(consulDiscoveryEvent);

          // We detect a failed connection attempt because the service
          // fails right away.
          if (event.connectTime + minConnectTime > System.currentTimeMillis()) {
            LOG
              .debug("Failure occurred soon after the discovery event was generated.  It will be classified as a " +
                "connection failure: {}", event);

            event.connectFailures++;

            if (maxReconnectAttempts > 0 && event.connectFailures >= maxReconnectAttempts) {
              LOG
                .warn("Reconnect attempts exceeded {} tries.  Reconnecting has been disabled for: {}",
                  maxReconnectAttempts, event);
              return;
            }

            if (!useExponentialBackOff || event.reconnectDelay == -1) {
              event.reconnectDelay = initialReconnectDelay;
            } else {
              // Exponential increment of reconnect delay.
              event.reconnectDelay *= backOffMultiplier;
              if (event.reconnectDelay > maxReconnectDelay) {
                event.reconnectDelay = maxReconnectDelay;
              }
            }

            doReconnectDelay(event);

          } else {
            LOG.trace("Failure occurred too long after the discovery event was generated.  " +
              "It will not be classified as a connection failure: {}", event);
            event.connectFailures = 0;
            event.reconnectDelay = initialReconnectDelay;

            doReconnectDelay(event);
          }

          if (!running.get()) {
            LOG.debug("Reconnecting disabled: stopped");
            return;
          }

          event.connectTime = System.currentTimeMillis();
          event.failed.set(false);
          listener.onServiceAdd(event);
          services.add(event.getService());
        }
      });
    }
  }

  private void doReconnectDelay(ConsulDiscoveryEvent event) {
    synchronized (sleepMutex) {
      try {
        if (!running.get()) {
          LOG.debug("Reconnecting disabled: stopped");
          return;
        }

        LOG.debug("Waiting {}ms before attempting to reconnect.", event.reconnectDelay);
        sleepMutex.wait(event.reconnectDelay);
      } catch (InterruptedException ie) {
        LOG.debug("Reconnecting disabled: ", ie);
        Thread.currentThread().interrupt();
      }
    }
  }

  public long getBackOffMultiplier() {
    return backOffMultiplier;
  }

  public void setBackOffMultiplier(long backOffMultiplier) {
    this.backOffMultiplier = backOffMultiplier;
  }

  public long getInitialReconnectDelay() {
    return initialReconnectDelay;
  }

  public void setInitialReconnectDelay(long initialReconnectDelay) {
    this.initialReconnectDelay = initialReconnectDelay;
  }

  public int getMaxReconnectAttempts() {
    return maxReconnectAttempts;
  }

  public void setMaxReconnectAttempts(int maxReconnectAttempts) {
    this.maxReconnectAttempts = maxReconnectAttempts;
  }

  public long getMaxReconnectDelay() {
    return maxReconnectDelay;
  }

  public void setMaxReconnectDelay(long maxReconnectDelay) {
    this.maxReconnectDelay = maxReconnectDelay;
  }

  public long getMinConnectTime() {
    return minConnectTime;
  }

  public void setMinConnectTime(long minConnectTime) {
    this.minConnectTime = minConnectTime;
  }

  public boolean isUseExponentialBackOff() {
    return useExponentialBackOff;
  }

  public void setUseExponentialBackOff(boolean useExponentialBackOff) {
    this.useExponentialBackOff = useExponentialBackOff;
  }

  class ConsulDiscoveryEvent extends DiscoveryEvent {

    private final AtomicBoolean failed = new AtomicBoolean(false);
    private int connectFailures;
    private long reconnectDelay = -1;
    private long connectTime = System.currentTimeMillis();
    private Service service;

    public ConsulDiscoveryEvent(Service service) {
      super("tcp://" + service.getAddress() + ":" + service.getPort());
      this.service = service;
    }

    public ConsulDiscoveryEvent(ConsulDiscoveryEvent copy) {
      super(copy);
      connectFailures = copy.connectFailures;
      reconnectDelay = copy.reconnectDelay;
      connectTime = copy.connectTime;
      failed.set(copy.failed.get());
    }

    public Service getService() {
      return service;
    }

    @Override
    public String toString() {
      return "[" + serviceName + ", failed:" + failed + ", connectionFailures:" + connectFailures + "]";
    }
  }
}

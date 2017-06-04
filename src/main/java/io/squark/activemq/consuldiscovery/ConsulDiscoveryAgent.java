package io.squark.activemq.consuldiscovery;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import io.squark.activemq.consuldiscovery.model.Check;
import io.squark.activemq.consuldiscovery.model.Service;
import io.squark.activemq.consuldiscovery.model.ServiceNode;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsulDiscoveryAgent implements DiscoveryAgent {

  private final static Logger LOG = LoggerFactory.getLogger(ConsulDiscoveryAgent.class);
  private final URI fullPath;
  private final Object sleepMutex = new Object();
  private final Object pollSleepMutex = new Object();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private long initialReconnectDelay = 1000;
  private long maxReconnectDelay = 1000 * 30;
  private long backOffMultiplier = 2;
  private boolean useExponentialBackOff = true;
  private int maxReconnectAttempts;
  private long minConnectTime = 5000;
  private DiscoveryListener listener;
  private Set<Service> services = new HashSet<Service>();
  private TaskRunnerFactory taskRunner;
  private long pollingDelay = 30000L;
  private ScheduledExecutorService pollRunner;
  private JsonDeserializer<Check.Status> statusJsonDeserializer = new JsonDeserializer<Check.Status>() {
    @Override
    public Check.Status deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws
      JsonParseException {
      return Check.Status.byName(json.getAsString());
    }
  };
  private Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
    .registerTypeAdapter(Check.Status.class, statusJsonDeserializer).create();
  private Runnable pollRunnable = new Runnable() {
    @Override
    public void run() {
      if (!running.get()) {
        LOG.debug("Polling disabled: stopped");
        Thread.currentThread().interrupt();
      }
      try {
        synchronized (pollSleepMutex) {
          pollServices();
        }
      } catch (Throwable t) {
        LOG.warn("Failed polling!", t);
      }
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
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService)?params\". Found " +
            consulURI);
      }
      URI baseURI = data.getComponents()[0];
      LOG.info("Consul base URI: " + baseURI);
      URI basePath = URISupport.removeQuery(baseURI);
      Map<String, String> parameters = URISupport.parseParameters(baseURI);
      if (!parameters.containsKey("service")) {
        throw new IOException(
          "Expected Consul URI in the format \"consul:(https://consul:8500/?service=myService)?params\". Found " +
            consulURI);
      }
      if (!basePath.toString().endsWith("/")) {
        basePath = basePath.resolve("/");
      }
      fullPath = basePath.resolve("v1/health/service/" + parameters.get("service"));
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
    LOG.info("Starting Consul Discovery Agent for " + fullPath);
    taskRunner = new TaskRunnerFactory("Consul Discovery Agent");
    taskRunner.init();
    running.set(true);

    pollRunner = Executors.newSingleThreadScheduledExecutor();
    pollRunner.scheduleWithFixedDelay(pollRunnable, 0, pollingDelay, TimeUnit.MILLISECONDS);
  }

  private void pollServices() {
    try {
      LOG.info("Will attempt Consul discovery from " + fullPath);
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fullPath.toURL().openStream()));
      List<ServiceNode> serviceList =
        gson.fromJson(bufferedReader, new TypeToken<ArrayList<ServiceNode>>() {}.getType());
      bufferedReader.close();
      LOG.info("Found (" + serviceList.size() + ") services.");
      List<ServiceNode> servicesToAdd = new ArrayList<ServiceNode>();
      List<ServiceNode> servicesToDelete = new ArrayList<ServiceNode>();
      for (ServiceNode serviceNode : serviceList) {
        if (serviceNode.isPassing()) {
          Long listenPort = Long.getLong("activemq.port", -1);
          String listenHost = System.getProperty("activemq.host");
          String resolvedHost = InetAddress.getLocalHost().getHostAddress();
          //If we are listening on the same address:port as the service, that means we found ourselves. Skip that.
          if (
            !(serviceNode.getService().getPort().equals(listenPort) &&
              (serviceNode.getService().getAddress().equals(listenHost) ||
                serviceNode.getService().getAddress().equals(resolvedHost)))
            ) {
            servicesToAdd.add(serviceNode);
          } else {
            LOG.info("Skipping self (" + serviceNode.getService() + ")");
          }
        } else {
          LOG.info("Service " + serviceNode.getService() + " is failing. Making sure it is deleted.");
          servicesToDelete.add(serviceNode);
        }
      }
      for (ServiceNode serviceNode : servicesToDelete) {
        if (services.contains(serviceNode.getService())) {
          LOG.info("Removing service " + serviceNode.getService() + " with failing checks " + serviceNode.getChecks());
          services.remove(serviceNode.getService());
          listener.onServiceRemove(new ConsulDiscoveryEvent(serviceNode.getService()));
        }
      }
      List<Service> servicesAdded = new ArrayList<Service>();
      for (ServiceNode serviceNode : servicesToAdd) {
        if (!services.contains(serviceNode.getService())) {
          LOG.info("Adding service " + serviceNode.getService());
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Consul Discovery Agent for " + fullPath);
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

  public long getPollingDelay() {
    return pollingDelay;
  }

  public void setPollingDelay(long pollingDelay) {
    this.pollingDelay = pollingDelay;
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

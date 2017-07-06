package io.squark.activemq.consuldiscovery;

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Created by Erik Håkansson on 2017-07-05.
 * Copyright 2017
 */
class ConsulHolder {
  private static final Object consulLock = new Object();
  private static ConsulHolder instance = null;
  private final ConsulDiscoveryAgent agent;
  private Consul consul;
  private Long lastTtl;
  private Long lastRegisterSelf;
  private ConcurrentHashMap<String, ServiceCache> cachedServices = new ConcurrentHashMap<String, ServiceCache>();

  private ConsulHolder(Consul consul, ConsulDiscoveryAgent agent) {
    this.consul = consul;
    this.agent = agent;
  }

  synchronized static ConsulHolder instance() {
    synchronized (consulLock) {
      if (instance == null) {
        throw new ConsulDiscoveryException("Not initialized");
      }
      return instance;
    }
  }

  synchronized void registerSelf(Registration registration) {
    synchronized (consulLock) {
      if (lastRegisterSelf == null || lastRegisterSelf < (System.currentTimeMillis() - ConsulDiscoveryAgent.pollingInterval)) {
        try {
          consul.agentClient().register(registration);
          lastRegisterSelf = System.currentTimeMillis();
        } catch (ConsulException e) {
          throw new ConsulDiscoveryException(e);
        }
      }
    }
  }

  synchronized void deregister(String consulServiceId) {
    synchronized (consulLock) {
      try {
        consul.agentClient().deregister(consulServiceId);
      } catch (ConsulException e) {
        throw new ConsulDiscoveryException(e);
      }
    }
  }

  synchronized void checkTtl(String consulServiceId) throws NotRegisteredException {
    synchronized (consulLock) {
      checkInstance();
      try {
        if (lastTtl == null || lastTtl < (System.currentTimeMillis() - ConsulDiscoveryAgent.pollingInterval)) {
          consul.agentClient().checkTtl(consulServiceId, State.PASS, null);
          lastTtl = System.currentTimeMillis();
        }
      } catch (NotRegisteredException e) {
        throw new ConsulDiscoveryException(e);
      } catch (ConsulException e) {
        throw new ConsulDiscoveryException(e);
      }
    }
  }

  List<ServiceHealth> getServices(String consulServiceName) {
    synchronized (consulLock) {
      checkInstance();
      try {
        ServiceCache cached = cachedServices.get(consulServiceName);
        if (cached == null || cached.when < (System.currentTimeMillis() - ConsulDiscoveryAgent.pollingInterval)) {
          HealthClient healthClient = consul.healthClient();
          List<ServiceHealth> services = healthClient.getAllServiceInstances(consulServiceName).getResponse();
          cached = new ServiceCache(System.currentTimeMillis(), services);
          cachedServices.put(consulServiceName, cached);
        }
        return cached.services;
      } catch (ConsulException e) {
        throw new ConsulDiscoveryException(e);
      }
    }
  }

  synchronized boolean hasInstance() {
    synchronized (consulLock) {
      return consul != null;
    }
  }

  synchronized static ConsulHolder initialize(Consul consul, ConsulDiscoveryAgent consulDiscoveryAgent) {
    synchronized (consulLock) {
      if (ConsulHolder.instance != null) {
        throw new ConsulDiscoveryException("Already intitialized");
      }
      instance = new ConsulHolder(consul, consulDiscoveryAgent);
      return instance;
    }
  }

  private void checkInstance() {
    if (instance == null) {
      throw new ConsulDiscoveryException("Not initialized");
    }
  }

  private class ServiceCache {
    private long when;
    private List<ServiceHealth> services;

    ServiceCache(long when, List<ServiceHealth> services) {
      this.when = when;
      this.services = services;
    }
  }
}

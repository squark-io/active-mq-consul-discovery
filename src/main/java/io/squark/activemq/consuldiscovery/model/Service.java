package io.squark.activemq.consuldiscovery.model;

import java.util.ArrayList;
import java.util.List;

public class Service {
  private String ID;
  private String service;
  private List<String> tags = new ArrayList<String>();
  private String address;
  private Long port;

  public String getID() {
    return ID;
  }

  public void setID(String ID) {
    this.ID = ID;
  }

  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public Long getPort() {
    return port;
  }

  public void setPort(Long port) {
    this.port = port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Service service1 = (Service) o;

    if (ID != null ? !ID.equals(service1.ID) : service1.ID != null) return false;
    return service != null ? service.equals(service1.service) : service1.service == null;
  }

  @Override
  public int hashCode() {
    int result = ID != null ? ID.hashCode() : 0;
    result = 31 * result + (service != null ? service.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Service{" +
      "ID='" + ID + '\'' +
      ", service='" + service + '\'' +
      ", tags=" + tags +
      ", address='" + address + '\'' +
      ", port=" + port +
      '}';
  }
}

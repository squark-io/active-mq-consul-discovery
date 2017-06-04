package io.squark.activemq.consuldiscovery.model;

import java.util.ArrayList;
import java.util.List;

public class Check {
  private String node;
  private String checkID;
  private String name;
  private Status status;
  private String notes;
  private String output;
  private String serviceID;
  private String serviceName;
  private List<String> serviceTags = new ArrayList<String>();

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getCheckID() {
    return checkID;
  }

  public void setCheckID(String checkID) {
    this.checkID = checkID;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }

  public String getOutput() {
    return output;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public String getServiceID() {
    return serviceID;
  }

  public void setServiceID(String serviceID) {
    this.serviceID = serviceID;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public List<String> getServiceTags() {
    return serviceTags;
  }

  public void setServiceTags(List<String> serviceTags) {
    this.serviceTags = serviceTags;
  }

  @Override
  public String toString() {
    return "Check{" +
      "name='" + name + '\'' +
      ", status=" + status +
      '}';
  }

  public enum Status {
    HealthAny("any", -2), HealthPassing("passing", 0), HealthWarning("warning", 1), HealthCritical("critical", 2),
    HealthMaint("maintenance", -1);

    private String name;
    private int ordinal;

    Status(String name, int ordinal) {
      this.name = name;
      this.ordinal = ordinal;
    }

    public static Status byName(String value) {
      for (Status candidate : Status.values()) {
        if (candidate.name.equals(value)) {
          return candidate;
        }
      }
      return null;
    }

    public boolean isWorseThan(Status other) {
      return this != HealthAny && this.ordinal > other.ordinal;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}

package io.squark.activemq.consuldiscovery.model;

import java.util.List;

public class ServiceNode {
  private Node node;
  private Service service;
  private List<Check> checks;

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public Service getService() {
    return service;
  }

  public void setService(Service service) {
    this.service = service;
  }

  public List<Check> getChecks() {
    return checks;
  }

  public void setChecks(List<Check> checks) {
    this.checks = checks;
  }

  public boolean isPassing() {
    for (Check check : checks) {
      if (check.getStatus().isWorseThan(Check.Status.HealthPassing)) {
        return false;
      }
    }
    return true;
  }
}

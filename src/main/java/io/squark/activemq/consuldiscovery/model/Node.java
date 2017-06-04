package io.squark.activemq.consuldiscovery.model;

import java.util.HashMap;
import java.util.Map;

public class Node {
  private String ID;
  private String node;
  private String address;
  private String datacenter;
  private Map<String, String> taggedAddresses = new HashMap<String, String>();
  private Map<String, String> meta = new HashMap<String, String>();

  public String getID() {
    return ID;
  }

  public void setID(String ID) {
    this.ID = ID;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public void setDatacenter(String datacenter) {
    this.datacenter = datacenter;
  }

  public Map<String, String> getTaggedAddresses() {
    return taggedAddresses;
  }

  public void setTaggedAddresses(Map<String, String> taggedAddresses) {
    this.taggedAddresses = taggedAddresses;
  }

  public Map<String, String> getMeta() {
    return meta;
  }

  public void setMeta(Map<String, String> meta) {
    this.meta = meta;
  }
}

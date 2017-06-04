# Consul Discovery Agent for ActiveMQ

Out of the box, [Apache ActiveMQ](http://activemq.apache.org/), supports a few Discovery protocols, e.g. [multicast, static and master-slave](http://activemq.apache.org/networks-of-brokers.html)

This library adds support for performing Discovery against a [Consul](https://www.consul.io/) agent, by specifying API access point and service name.

Currently, the library is in a proof-of-concept state and has only been tested against [Jboss A-MQ](https://developers.redhat.com/products/amq/overview/) but should work fine with vanilla ActiveMQ as well.

## Install
### Jboss A-MQ
```bash
mvn clean install
cp target/active-mq-consul-discovery-[VERSION]-jar-with-dependencies.jar [JBOSS A-MQ HOME]/deploy/
```

## Configure
Configure a network connector with `consul` as URI in `activemq.xml`
```xml
<networkConnectors>
  <networkConnector uri="consul:(http://consul.example.com?service=active-mq)"/>
</networkConnectors>
```
`service` is the only currently supported parameter on the Consul component itself, however, the same parameters as those used for static networks can be used and will be applied to each service instance.
E.g:
```xml
<networkConnector uri="consul:(http://consul.example.com?service=active-mq)?initialReconnectDelay=500&amp;maxReconnectDelay=10000"/>
```

## Contributions
Feel free to make feature requests or preferably pull requests!

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

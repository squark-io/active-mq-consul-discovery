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
### Network of brokers
Configure a network connector with `consul` as URI in `activemq.xml`
```xml
<networkConnectors>
  <networkConnector uri="consul:(http://consul.example.com?service=active-mq)"/>
</networkConnectors>
```
This will perform discovery against http://consul.example.com/v1/health/service/active-mq, i.e. the actual API endpoint is automatically appended and should NOT be included in the URI.
The following parameters are required:
* service\t(the name of the service to register with Consul)
* address\t(the address to the service, i.e the host at which A-MQ listens)
* port\t(the port on which A-MQ listens)
The following parameters are supported but not required:
* serviceId\t(the ID for this instance of the service. Will be randomly generated if left out.)
* maxQuarantineTime\t(The maximum time to quarantine a failing peer service that is still available in Consul)
* interval\t(The interval with which to poll Consul for service instances)

`service` is the only currently supported parameter on the Consul component itself, however, the same parameters as those used for static networks can be used and will be applied to each service instance.
E.g:
```xml
<networkConnector uri="consul:(http://consul.example.com?service=active-mq&amp;address=amq.example.com&amp;port=61616)"/>
```
### Client-side
This has not been tested yet, but in theory, it should work with
```
<transportConnectors>
  <transportConnector name="default" uri="discovery:(consul:(http://consul.example.com?service=active-mq&amp;address=amq.example.com&amp;port=61616))"/>
</transportConnectors>
```

## Road-map
Planned items are:
* Proper client-side testing
* Maven releases

## Contributions
Feel free to make feature requests or preferably pull requests!

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Gateway
==================

## Overview

In the Event Sourcing pattern, the interaction with the external interfaces is carried out by gateways. This library facilitate the develop of gateways.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:gateway-lib:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>gateway-lib</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```

## Usage and examples

### Configuration

This library use a extended [common-lib](../archer-common-lib/README.md) configuration system with a annotation Config. For configure a gateway you should annotate a each gateway class with Config annotation. For example:
```java
@Config(file = "gateway.yml", servicesPackage = "com.example")
```

### Initialization

For create a own gateway you need to extends Gateway.
```java
@Config(file = "example.yml", servicesPackage = "com.example")
class ExampleGateway extends Gateway {
    ExampleGateway() {
        configure(ExampleGateway.class);
        domain = new Domain(new ExampleHandler(servicesPackage, config), config.getApplicationConfig());
        start();
        new HelperApplication(config.getApplicationConfig());
    }

}
```

And implement the services with the ServiceConfig annotation.
```java
@ServiceConfig(file = "services/service_example.yml")
public class ExampleService extends GatewayService<DataBean> {

    @Override
    public void postInitActions() {
    }

    @Override
    public DataBean call(final CRecord record) {
        //Call to external service
        return new TransactionDataBean();
    }

    @Override
    protected Boolean isSuccess(final DataBean result) {
        return true; //Check in the result
    }

    @Override
    public void processResult(final CRecord cRecord, final DataBean dataBean) {

    }
}
```

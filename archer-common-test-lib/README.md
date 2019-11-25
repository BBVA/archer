Common lib test
==================

## Overview

This is the common test library to get up a complete kafka stack in memory and simulate the real event bus infrastructure in the test environment.

It releases a complete kafka stack and schema registry.

## Requirements

* [Java](https://www.java.com) >= 1.8

## Installation

Gradle
```text/plain
# build.gradle
dependencies {
	compile('bbva.ndb:lib-common-test:1.0.0-beta.2')
}
```

Maven
```text/plain
# pom.xml
<dependencies>
    <dependency>
        <groupId>bbva.ndb</groupId>
        <artifactId>lib-common-test</artifactId>
        <version>1.0.0-beta.2</version>
    </dependency>
</dependencies>
```


## Usage and examples
To use the kafka stack in memory you only need to extends of BaseItTest.
And automatically in this class we have a populated ApplicationConfig with all configurations. At this moment we can consume, produce and do streams in front of memory stack.
```java
public class MyTest extends BaseItTest {

    @Before
    public void before() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new EventConsumer<Data>(10, Collections.singletonList("data_events"), testsConsumerCallback, appConfig));
    }
}
```

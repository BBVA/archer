package com.bbva.example;

import com.bbva.gateway.Gateway;
import com.bbva.gateway.config.annotations.Config;

@Config(file = "example/multiservices.yml", servicesPackage = "com.bbva.example.service")
public class MultiservicesGateway extends Gateway {

}

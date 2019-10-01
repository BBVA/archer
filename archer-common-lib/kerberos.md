## Run securize services services

Configure bootstrap server with sasl protocol, for example
```
export ARCHER_BOOTSTRAP_SERVERS=SASL_SSL://KICKSTARTTEAM.COM:19094
```
Annotate service with @SecureConfig

Configure new environment variable with jass config, for example:
```
export JASS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="__KEYTAB_DIR__/kafka.keytab" principal="kafka/kafka@KICKSTARTTEAM.COM";
```

Specify the location of the truststore in the service launcher, for example:
```
-Djavax.net.ssl.trustStore=__TRUSTSTORE_DIR__/kafka.consumer.keystore.jks
```

Note: Is necesary configure kerberos in the clientes with any conf file similar to it:
```
[logging]
 default = FILE:/var/log/kerberos/krb5libs.log
 kdc = FILE:/var/log/kerberos/krb5kdc.log
 admin_server = FILE:/var/log/kerberos/kadmind.log

[libdefaults]
 default_realm = KICKSTARTTEAM.COM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 default_tkt_enctypes = rc4-hmac
 default_tgs_enctypes = rc4-hmac

[realms]
 KICKSTARTTEAM.COM = {
  kdc = localhost
  admin_server = localhost
 }

[domain_realm]
 .localhost = KICKSTARTTEAM.COM
 locahost = KICKSTARTTEAM.COM
```

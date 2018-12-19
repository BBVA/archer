package com.bbva.common.utils;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.*;

public class CustomCachedSchemaRegistryClient implements SchemaRegistryClient {

    private final RestService restService;
    private final int identityMapCapacity;
    private final Map<String, Map<Schema, Integer>> schemaCache;
    private final Map<String, Map<Integer, Schema>> idCache;
    private final Map<String, Map<Schema, Integer>> versionCache;
    public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES = new HashMap();

    public CustomCachedSchemaRegistryClient(final String baseUrl, final int identityMapCapacity) {
        this(new RestService(baseUrl), identityMapCapacity);
    }

    public CustomCachedSchemaRegistryClient(final List<String> baseUrls, final int identityMapCapacity) {
        this(new RestService(baseUrls), identityMapCapacity);
    }

    public CustomCachedSchemaRegistryClient(final RestService restService, final int identityMapCapacity) {
        this.identityMapCapacity = identityMapCapacity;
        this.schemaCache = new HashMap<>();
        this.idCache = new HashMap<>();
        this.versionCache = new HashMap<>();
        this.restService = restService;
        this.idCache.put(null, new HashMap<>());
    }

    private int registerAndGetId(final String subject, final Schema schema) throws IOException, RestClientException {
        return restService.registerSchema(schema.toString(), subject);
    }

    private Schema getSchemaByIdFromRegistry(final int id) throws IOException, RestClientException {
        final SchemaString restSchema = restService.getId(id);
        return new Schema.Parser().parse(restSchema.getSchemaString());
    }

    private int getVersionFromRegistry(final String subject, final Schema schema)
            throws IOException, RestClientException {
        final io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = restService
                .lookUpSubjectVersion(schema.toString(), subject, true);
        return response.getVersion();
    }

    private int getIdFromRegistry(final String subject, final Schema schema) throws IOException, RestClientException {
        final io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = restService
                .lookUpSubjectVersion(schema.toString(), subject, false);
        return response.getId();
    }

    private static int getSchema(final Map<Schema, Integer> schemaIdMap, final Object schema) {
        int version = -1;
        for (final Object key : schemaIdMap.keySet().toArray()) {
            if (key.equals(schema)) {
                version = schemaIdMap.get((Schema) key);
                break;
            }
        }
        return version;
    }

    @Override
    public synchronized int register(final String subject, final Schema schema)
            throws IOException, RestClientException {
        final Map<Schema, Integer> schemaIdMap;
        if (schemaCache.containsKey(subject)) {
            schemaIdMap = schemaCache.get(subject);
        } else {
            schemaIdMap = new IdentityHashMap<>();
            schemaCache.put(subject, schemaIdMap);
        }

        int id = getSchema(schemaIdMap, schema);
        if (id > -1) {
            return id;
        } else if (schemaIdMap.containsKey(schema)) {
            return schemaIdMap.get(schema);
        } else {
            if (schemaIdMap.size() >= identityMapCapacity) {
                throw new IllegalStateException("Too many schema objects created for " + subject + "!");
            }
            id = registerAndGetId(subject, schema);
            schemaIdMap.put(schema, id);
            idCache.get(null).put(id, schema);
            return id;
        }
    }

    @Override
    public Schema getByID(final int id) throws IOException, RestClientException {
        return getById(id);
    }

    @Override
    public synchronized Schema getById(final int id) throws IOException, RestClientException {
        return getBySubjectAndId(null, id);
    }

    @Override
    public Schema getBySubjectAndID(final String subject, final int id) throws IOException, RestClientException {
        return getBySubjectAndId(subject, id);
    }

    @Override
    public synchronized Schema getBySubjectAndId(final String subject, final int id)
            throws IOException, RestClientException {

        final Map<Integer, Schema> idSchemaMap;
        if (idCache.containsKey(subject)) {
            idSchemaMap = idCache.get(subject);
        } else {
            idSchemaMap = new HashMap<>();
            idCache.put(subject, idSchemaMap);
        }

        if (idSchemaMap.containsKey(id)) {
            return idSchemaMap.get(id);
        } else {
            final Schema schema = getSchemaByIdFromRegistry(id);
            idSchemaMap.put(id, schema);
            return schema;
        }
    }

    @Override
    public SchemaMetadata getSchemaMetadata(final String subject, final int version)
            throws IOException, RestClientException {
        final io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = restService.getVersion(subject,
                version);
        final int id = response.getId();
        final String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    @Override
    public synchronized SchemaMetadata getLatestSchemaMetadata(final String subject)
            throws IOException, RestClientException {
        final io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = restService
                .getLatestVersion(subject);
        final int id = response.getId();
        final int version = response.getVersion();
        final String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    @Override
    public synchronized int getVersion(final String subject, final Schema schema)
            throws IOException, RestClientException {
        final Map<Schema, Integer> schemaVersionMap;
        if (versionCache.containsKey(subject)) {
            schemaVersionMap = versionCache.get(subject);
        } else {
            schemaVersionMap = new IdentityHashMap<>();
            versionCache.put(subject, schemaVersionMap);
        }

        if (schemaVersionMap.containsKey(schema)) {
            return schemaVersionMap.get(schema);
        } else {
            if (schemaVersionMap.size() >= identityMapCapacity) {
                throw new IllegalStateException("Too many schema objects created for " + subject + "!");
            }
            final int version = getVersionFromRegistry(subject, schema);
            schemaVersionMap.put(schema, version);
            return version;
        }
    }

    @Override
    public List<Integer> getAllVersions(final String subject) throws IOException, RestClientException {
        return this.restService.getAllVersions(subject);
    }

    @Override
    public synchronized int getId(final String subject, final Schema schema) throws IOException, RestClientException {
        final Map<Schema, Integer> schemaIdMap;
        if (schemaCache.containsKey(subject)) {
            schemaIdMap = schemaCache.get(subject);
        } else {
            schemaIdMap = new IdentityHashMap<>();
            schemaCache.put(subject, schemaIdMap);
        }

        if (schemaIdMap.containsKey(schema)) {
            return schemaIdMap.get(schema);
        } else {
            if (schemaIdMap.size() >= identityMapCapacity) {
                throw new IllegalStateException("Too many schema objects created for " + subject + "!");
            }
            final int id = getIdFromRegistry(subject, schema);
            schemaIdMap.put(schema, id);
            idCache.get(null).put(id, schema);
            return id;
        }
    }

    @Override
    public List<Integer> deleteSubject(final String subject) throws IOException, RestClientException {
        return this.deleteSubject(DEFAULT_REQUEST_PROPERTIES, subject);
    }

    @Override
    public List<Integer> deleteSubject(final Map<String, String> requestProperties, final String subject)
            throws IOException, RestClientException {
        this.versionCache.remove(subject);
        this.idCache.remove(subject);
        this.schemaCache.remove(subject);
        return this.restService.deleteSubject(requestProperties, subject);
    }

    @Override
    public Integer deleteSchemaVersion(final String subject, final String version)
            throws IOException, RestClientException {
        return this.deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES, subject, version);
    }

    @Override
    public Integer deleteSchemaVersion(final Map<String, String> requestProperties, final String subject,
            final String version) throws IOException, RestClientException {
        ((Map) this.versionCache.get(subject)).values().remove(Integer.valueOf(version));
        return this.restService.deleteSchemaVersion(requestProperties, subject, version);
    }

    @Override
    public boolean testCompatibility(final String subject, final Schema schema)
            throws IOException, RestClientException {
        return restService.testCompatibility(schema.toString(), subject, "latest");
    }

    @Override
    public String updateCompatibility(final String subject, final String compatibility)
            throws IOException, RestClientException {
        final ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
        return response.getCompatibilityLevel();
    }

    @Override
    public String getCompatibility(final String subject) throws IOException, RestClientException {
        final Config response = restService.getConfig(subject);
        return response.getCompatibilityLevel();
    }

    @Override
    public Collection<String> getAllSubjects() throws IOException, RestClientException {
        return restService.getAllSubjects();
    }

    static {
        DEFAULT_REQUEST_PROPERTIES.put("Content-Type", "application/vnd.schemaregistry.v1+json");
    }

}

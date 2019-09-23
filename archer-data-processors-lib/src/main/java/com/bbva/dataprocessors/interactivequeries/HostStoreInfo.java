/**
 * Copyright 2016 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.bbva.dataprocessors.interactivequeries;

import java.util.Objects;
import java.util.Set;

/**
 * Information of stores
 */
public class HostStoreInfo {

    private String host;
    private int port;
    private Set<String> storeNames;

    /**
     * Constructor
     */
    public HostStoreInfo() {
    }

    /**
     * Constructor
     *
     * @param host       host
     * @param port       port
     * @param storeNames store names
     */
    public HostStoreInfo(final String host, final int port, final Set<String> storeNames) {
        this.host = host;
        this.port = port;
        this.storeNames = storeNames;
    }

    /**
     * Get host name
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Set the host name
     *
     * @param host host value
     */
    public void setHost(final String host) {
        this.host = host;
    }

    /**
     * Get the port
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set port number
     *
     * @param port port
     */
    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * Get store names
     *
     * @return names
     */
    public Set<String> getStoreNames() {
        return storeNames;
    }

    /**
     * Set unique store names
     *
     * @param storeNames
     */
    public void setStoreNames(final Set<String> storeNames) {
        this.storeNames = storeNames;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "HostStoreInfo{" + "host='" + host + '\'' + ", port=" + port + ", storeNames=" + storeNames + '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HostStoreInfo that = (HostStoreInfo) o;
        return port == that.port && Objects.equals(host, that.host) && Objects.equals(storeNames, that.storeNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(host, port, storeNames);
    }
}

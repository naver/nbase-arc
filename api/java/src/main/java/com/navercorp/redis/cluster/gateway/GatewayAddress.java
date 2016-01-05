/*
 * Copyright 2015 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.navercorp.redis.cluster.gateway;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class GatewayAddress.
 *
 * @author jaehong.kim
 */
public class GatewayAddress {

    /**
     * The name.
     */
    private final String name;

    /**
     * The host.
     */
    private final String host;

    /**
     * The port.
     */
    private final int port;

    private int id;

    /**
     * As list.
     *
     * @param addresses the addresses
     * @return the list
     */
    public static List<GatewayAddress> asList(final String addresses) {
        if (addresses == null) {
            throw new IllegalArgumentException("addresses must not be null");
        }

        List<GatewayAddress> list = new ArrayList<GatewayAddress>();
        final String[] tokens = addresses.split(",");
        if (tokens == null) {
            return list;
        }

        int id = 1;
        for (String token : tokens) {
            list.add(new GatewayAddress(id++, token));
        }

        return list;
    }

    /**
     * As list.
     *
     * @param addressList the addresses
     * @return the list
     */
    public static List<GatewayAddress> asList(final List<String> addressList) {
        if (addressList == null || addressList.size() == 0) {
            throw new IllegalArgumentException("addresses must not be null");
        }

        List<GatewayAddress> list = new ArrayList<GatewayAddress>();

        int id = 1;
        for (String address : addressList) {
            list.add(new GatewayAddress(id++, address));
        }

        return list;
    }

    /**
     * As list from domain.
     *
     * @param domainAddress the domain address
     * @return the list
     */
    public static List<GatewayAddress> asListFromDomain(final String domainAddress) {
        if (domainAddress == null) {
            throw new IllegalArgumentException("domain address must not be null");
        }

        GatewayAddress domain = new GatewayAddress(0, domainAddress);
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(domain.getHost());
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid domain '" + domain + "' " + e.getMessage());
        }

        List<GatewayAddress> list = new ArrayList<GatewayAddress>();
        int id = 1;
        for (InetAddress address : addresses) {
            list.add(new GatewayAddress(id++, address.getHostAddress(), domain.getPort()));
        }

        return list;
    }

    /**
     * Instantiates a new gateway address.
     *
     * @param address the address
     */
    public GatewayAddress(final int id, final String address) {
        this.id = id;
        this.name = address;
        this.host = parseHost(address);
        this.port = parsePort(address);
    }

    /**
     * Instantiates a new gateway address.
     *
     * @param host the host
     * @param port the port
     */
    public GatewayAddress(final int id, final String host, final int port) {
        this.id = id;
        this.name = host + ":" + port;
        this.host = host;
        this.port = port;
    }

    /**
     * Parses the host.
     *
     * @param address the address
     * @return the string
     */
    public static String parseHost(final String address) {
        int ep = address.indexOf(":");
        if (ep == -1 || ep == 0) {
            throw new IllegalArgumentException("invalid address '" + address + "'");
        }
        return address.substring(0, ep).trim();
    }

    /**
     * Parses the port.
     *
     * @param address the address
     * @return the int
     */
    public static int parsePort(final String address) {
        int sp = address.indexOf(":");
        if (sp == -1 && sp + 1 >= address.length()) {
            throw new IllegalArgumentException("not found port '" + address + "'");
        }

        try {
            return Integer.parseInt(address.substring(sp + 1, address.length()).trim());
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("bad port number '" + address + "'");
        }
    }

    /**
     * Gets the host.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    /*
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return this.name;
    }
}
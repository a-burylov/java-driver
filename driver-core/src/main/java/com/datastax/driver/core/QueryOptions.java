/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

/**
 * Options related to defaults for individual queries.
 */
public class QueryOptions {

    /**
     * The default consistency level for queries: {@link ConsistencyLevel#ONE}.
     */
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    /**
     * The default serial consistency level for conditional updates: {@link ConsistencyLevel#SERIAL}.
     */
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;

    /**
     * The default fetch size for SELECT queries: 5000.
     */
    public static final int DEFAULT_FETCH_SIZE = 5000;

    /**
     * The default value for {@link #getDefaultIdempotence()}: {@code false}.
     */
    public static final boolean DEFAULT_IDEMPOTENCE = false;

    public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS = 20;

    public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS = 20;

    public static final int DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS = 20;

    public static final int DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS = 1000;

    public static final int DEFAULT_REFRESH_NODE_INTERVAL_MILLIS = 1000;

    public static final int DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS = 1000;


    private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
    private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
    private volatile int fetchSize = DEFAULT_FETCH_SIZE;
    private volatile boolean defaultIdempotence = DEFAULT_IDEMPOTENCE;

    private volatile int maxPendingRefreshNodeListRequests = DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS;
    private volatile int maxPendingRefreshNodeRequests = DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS;
    private volatile int maxPendingRefreshSchemaRequests = DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS;

    private volatile int refreshNodeListIntervalMillis = DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS;
    private volatile int refreshNodeIntervalMillis = DEFAULT_REFRESH_NODE_INTERVAL_MILLIS;
    private volatile int refreshSchemaIntervalMillis = DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS;

    private volatile Cluster.Manager manager;

    /**
     * Creates a new {@link QueryOptions} instance using the {@link #DEFAULT_CONSISTENCY_LEVEL},
     * {@link #DEFAULT_SERIAL_CONSISTENCY_LEVEL} and {@link #DEFAULT_FETCH_SIZE}.
     */
    public QueryOptions() {}

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Sets the default consistency level to use for queries.
     * <p>
     * The consistency level set through this method will be use for queries
     * that don't explicitly have a consistency level, i.e. when {@link Statement#getConsistencyLevel}
     * returns {@code null}.
     *
     * @param consistencyLevel the new consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistency = consistencyLevel;
        return this;
    }

    /**
     * The default consistency level used by queries.
     *
     * @return the default consistency level used by queries.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Sets the default serial consistency level to use for queries.
     * <p>
     * The serial consistency level set through this method will be use for queries
     * that don't explicitly have a serial consistency level, i.e. when {@link Statement#getSerialConsistencyLevel}
     * returns {@code null}.
     *
     * @param serialConsistencyLevel the new serial consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        this.serialConsistency = serialConsistencyLevel;
        return this;
    }

    /**
     * The default serial consistency level used by queries.
     *
     * @return the default serial consistency level used by queries.
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    /**
     * Sets the default fetch size to use for SELECT queries.
     * <p>
     * The fetch size set through this method will be use for queries
     * that don't explicitly have a fetch size, i.e. when {@link Statement#getFetchSize}
     * is less or equal to 0.
     *
     * @param fetchSize the new fetch size to set as default. It must be
     * strictly positive but you can use {@code Integer.MAX_VALUE} to disable
     * paging.
     * @return this {@code QueryOptions} instance.
     *
     * @throws IllegalArgumentException if {@code fetchSize &lte; 0}.
     * @throws UnsupportedFeatureException if version 1 of the native protocol is in
     * use and {@code fetchSize != Integer.MAX_VALUE} as paging is not supported by
     * version 1 of the protocol. See {@link Cluster.Builder#withProtocolVersion}
     * for more details on protocol versions.
     */
    public QueryOptions setFetchSize(int fetchSize) {
        if (fetchSize <= 0)
            throw new IllegalArgumentException("Invalid fetchSize, should be > 0, got " + fetchSize);

        int version = manager == null ? -1 : manager.protocolVersion();
        if (fetchSize != Integer.MAX_VALUE && version == 1)
            throw new UnsupportedFeatureException("Paging is not supported");

        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The default fetch size used by queries.
     *
     * @return the default fetch size used by queries.
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Sets the default idempotence for queries.
     * <p>
     * This will be used for statements for which {@link com.datastax.driver.core.Statement#isIdempotent()}
     * returns {@code null}.
     *
     * @param defaultIdempotence the new value to set as default idempotence.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setDefaultIdempotence(boolean defaultIdempotence) {
        this.defaultIdempotence = defaultIdempotence;
        return this;
    }

    /**
     * The default idempotence for queries.
     * <p>
     * It defaults to {@link #DEFAULT_IDEMPOTENCE}.
     *
     * @return the default idempotence for queries.
     */
    public boolean getDefaultIdempotence() {
        return defaultIdempotence;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node list refresh requests.
     * <p>
     * When the control connection receives a new schema refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshSchemaIntervalMillis The default window size in milliseconds used to debounce schema refresh requests.
     */
    public QueryOptions setRefreshSchemaIntervalMillis(int refreshSchemaIntervalMillis) {
        this.refreshSchemaIntervalMillis = refreshSchemaIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce schema refresh requests.
     *
     * @return The default window size in milliseconds used to debounce schema refresh requests.
     */
    public int getRefreshSchemaIntervalMillis() {
        return refreshSchemaIntervalMillis;
    }

    /**
     * Sets the maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     * <p>
     * When the control connection receives a new schema refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshSchemaRequests The maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     */
    public QueryOptions setMaxPendingRefreshSchemaRequests(int maxPendingRefreshSchemaRequests) {
        this.maxPendingRefreshSchemaRequests = maxPendingRefreshSchemaRequests;
        return this;
    }

    /**
     * The maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshSchemaRequests() {
        return maxPendingRefreshSchemaRequests;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node list refresh requests.
     * <p>
     * When the control connection receives a new node list refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshNodeListIntervalMillis The default window size in milliseconds used to debounce node list refresh requests.
     */
    public QueryOptions setRefreshNodeListIntervalMillis(int refreshNodeListIntervalMillis) {
        this.refreshNodeListIntervalMillis = refreshNodeListIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce node list refresh requests.
     *
     * @return The default window size in milliseconds used to debounce node list refresh requests.
     */
    public int getRefreshNodeListIntervalMillis() {
        return refreshNodeListIntervalMillis;
    }

    /**
     * Sets the maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     * <p>
     * When the control connection receives a new node list refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshNodeListRequests The maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     */
    public QueryOptions setMaxPendingRefreshNodeListRequests(int maxPendingRefreshNodeListRequests) {
        this.maxPendingRefreshNodeListRequests = maxPendingRefreshNodeListRequests;
        return this;
    }

    /**
     * Sets the maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshNodeListRequests() {
        return maxPendingRefreshNodeListRequests;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node refresh requests.
     * <p>
     * When the control connection receives a new node refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshNodeIntervalMillis The default window size in milliseconds used to debounce node refresh requests.
     */
    public QueryOptions setRefreshNodeIntervalMillis(int refreshNodeIntervalMillis) {
        this.refreshNodeIntervalMillis = refreshNodeIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce node refresh requests.
     *
     * @return The default window size in milliseconds used to debounce node refresh requests.
     */
    public int getRefreshNodeIntervalMillis() {
        return refreshNodeIntervalMillis;
    }

    /**
     * Sets the maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     * <p>
     * When the control connection receives a new node refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshNodeRequests The maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     */
    public QueryOptions setMaxPendingRefreshNodeRequests(int maxPendingRefreshNodeRequests) {
        this.maxPendingRefreshNodeRequests = maxPendingRefreshNodeRequests;
        return this;
    }

    /**
     * The maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshNodeRequests() {
        return maxPendingRefreshNodeRequests;
    }

}

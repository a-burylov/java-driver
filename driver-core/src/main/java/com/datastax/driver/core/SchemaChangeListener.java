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

/**
 * Interface for objects that are interested in tracking schema change events in the cluster.
 * <p>
 * An implementation of this interface can be registered against a Cluster
 * object through the {@link com.datastax.driver.core.Cluster#register(SchemaChangeListener)} method.
 */
public interface SchemaChangeListener {

    /**
     * Called when a keyspace has been added.
     *
     * @param keyspace the keyspace that has been added.
     */
    void onKeyspaceAdded(KeyspaceMetadata keyspace);

    /**
     * Called when a keyspace has been removed.
     *
     * @param keyspace the keyspace that has been removed.
     */
    void onKeyspaceRemoved(KeyspaceMetadata keyspace);

    /**
     * Called when a keyspace has changed.
     *
     * @param current the keyspace that has changed, in its current form (after the change).
     * @param previous the keyspace that has changed, in its previous form (before the change).
     */
    void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous);

    /**
     * Called when a table has been added.
     *
     * @param table the table that has been newly added.
     */
    void onTableAdded(TableMetadata table);

    /**
     * Called when a table has been removed.
     *
     * @param table the table that has been removed.
     */
    void onTableRemoved(TableMetadata table);

    /**
     * Called when a table has changed.
     *
     * @param current the table that has changed, in its current form (after the change).
     * @param previous the table that has changed, in its previous form (before the change).
     */
    void onTableChanged(TableMetadata current, TableMetadata previous);

}

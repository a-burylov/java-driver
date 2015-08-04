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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import static com.datastax.driver.core.CCMBridge.ipOfNode;

public class EventDebouncerIntegrationTest {

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE test";

    /**
     * Tests that several schema change events will be debounced
     * and only one event will be received.
     *
     * @throws InterruptedException
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_schema_change_events() throws InterruptedException {
        TestSchemaChangeListener listener = new TestSchemaChangeListener();
        CCMBridge ccm = CCMBridge.builder("main").withNodes(1).build();
        final Cluster cluster = new Cluster.Builder()
            .addContactPoints(ipOfNode(1))
            .withQueryOptions(new QueryOptions()
                    .setRefreshNodeIntervalMillis(0)
                    .setRefreshNodeListIntervalMillis(0)
                    .setRefreshSchemaIntervalMillis(100)
            ).build();
        try {
            cluster.register(listener);
            Session session = cluster.connect();
            listener.start();
            for (int i = 0; i < 50; i++) {
                session.execute(CREATE_KEYSPACE);
                session.execute(DROP_KEYSPACE);
            }
            session.execute(CREATE_KEYSPACE);
            listener.awaitKeyspaceAdded();
        } finally {
            if(cluster != null)
                cluster.close();
            ccm.remove("main");
        }
    }

    /**
     * Tests that DOWN, UP, REMOVE or ADD events will not be delivered to
     * load balancing policy nor host state listeners
     * before the cluster is fully initialized.
     *
     * @throws InterruptedException
     * @jira_ticket JAVA-784
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_wait_until_load_balancing_policy_is_fully_initialized() throws InterruptedException {
        TestLoadBalancingPolicy policy = new TestLoadBalancingPolicy();
        CCMBridge ccm = CCMBridge.builder("main").withNodes(3).build();
        final Cluster cluster = new Cluster.Builder()
            .addContactPoints(ipOfNode(1))
            .withLoadBalancingPolicy(policy)
            .withQueryOptions(new QueryOptions()
                    .setRefreshNodeIntervalMillis(0)
                    .setRefreshNodeListIntervalMillis(0)
                    .setRefreshSchemaIntervalMillis(0)
            ).build();
        try {
            new Thread() {
                @Override
                public void run() {
                    cluster.init();
                }
            }.start();
            // stop cluster initialization in the middle of LBP initialization
            policy.stop();
            // generate a DOWN event - will not be delivered immediately
            // because the debouncers are not started
            ccm.stop(3);
            ccm.waitForDown(3);
            // finish cluster initialization and deliver the DOWN event
            policy.proceed();
            assertThat(policy.onDownCalled()).isTrue();
            assertThat(policy.hosts).doesNotContain(TestUtils.findHost(cluster, 3));
        } finally {
            if(cluster != null)
                cluster.close();
            ccm.remove("main");
        }
    }

    private class TestLoadBalancingPolicy extends SortingLoadBalancingPolicy {

        CyclicBarrier stop = new CyclicBarrier(2);

        CyclicBarrier proceed = new CyclicBarrier(2);

        CountDownLatch onDownCalled = new CountDownLatch(1);

        volatile boolean init = false;

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            try {
                stop.await(1, TimeUnit.MINUTES);
                proceed.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            super.init(cluster, hosts);
            init = true;
        }


        @Override
        public void onDown(Host host) {
            if (!init)
                fail("Should have waited until load balancing is fully initialized before delivering events");
            super.onDown(host);
            if(host.getAddress().toString().contains(ipOfNode(3)))
                onDownCalled.countDown();
        }

        public void stop() throws InterruptedException {
            try {
                stop.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        public void proceed() throws InterruptedException {
            try {
                proceed.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        boolean onDownCalled() throws InterruptedException {
            return onDownCalled.await(1, TimeUnit.MINUTES);
        }

    }

    static class TestSchemaChangeListener implements SchemaChangeListener {

        CountDownLatch keyspaceAdded;

        public void start() {
            keyspaceAdded = new CountDownLatch(1);
        }

        void awaitKeyspaceAdded() throws InterruptedException {
            keyspaceAdded.await();
        }

        @Override
        public void onKeyspaceAdded(KeyspaceMetadata keyspace) {
            // happens on initial connection
            if (keyspaceAdded == null)
                return;
            if(keyspaceAdded.getCount() == 0)
                fail("Should have debounced ADD KEYSPACE events");
            keyspaceAdded.countDown();
        }

        @Override
        public void onKeyspaceRemoved(KeyspaceMetadata keyspace) {
            fail("Should have debounced DROP KEYSPACE events");
        }

        @Override
        public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {
        }

        @Override
        public void onTableAdded(TableMetadata table) {
        }

        @Override
        public void onTableRemoved(TableMetadata table) {
        }

        @Override
        public void onTableChanged(TableMetadata current, TableMetadata previous) {
        }

    }
}

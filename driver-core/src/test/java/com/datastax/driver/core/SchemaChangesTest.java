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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.*;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Metadata.handleId;
import static com.datastax.driver.core.SchemaChangesTest.EventType.*;

public class SchemaChangesTest {

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }";
    private static final String ALTER_KEYSPACE = "ALTER KEYSPACE %s WITH durable_writes = false";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE %s";

    private static final String CREATE_TABLE = "CREATE TABLE %s.table1(i int primary key)";
    private static final String ALTER_TABLE = "ALTER TABLE %s.table1 ADD j int";
    private static final String DROP_TABLE = "DROP TABLE %s.table1";

    CCMBridge ccm;

    Cluster cluster1;
    Cluster cluster2; // a second cluster to check that other clients also get notified

    // The metadatas of the two clusters (we'll test that they're kept in sync)
    List<Metadata> metadatas;

    Session session;

    SchemaChangeTestListener listener1;
    SchemaChangeTestListener listener2;

    List<SchemaChangeTestListener> listeners;

    @BeforeClass(groups = "short")
    public void setup() throws InterruptedException {
        ccm = CCMBridge.builder("schemaChangesTest").withNodes(1).build();

        cluster1 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
        cluster2 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();

        metadatas = Lists.newArrayList(cluster1.getMetadata(), cluster2.getMetadata());

        session = cluster1.connect();

        cluster1.register(listener1 = spy(new SchemaChangeTestListener()));
        cluster1.register(listener2 = spy(new SchemaChangeTestListener()));
        listeners = Lists.newArrayList(listener1, listener2);

        execute(CREATE_KEYSPACE, "lowercase", KS_ADDED);
        execute(CREATE_KEYSPACE, "\"CaseSensitive\"", KS_ADDED);

    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster1 != null)
            cluster1.close();
        if (cluster2 != null)
            cluster2.close();
        if (ccm != null)
            ccm.remove();
    }

    @DataProvider(name = "existingKeyspaceName")
    public static Object[][] existingKeyspaceName() {
        return new Object[][]{ { "lowercase" }, { "\"CaseSensitive\"" } };
    }

    @DataProvider(name = "newKeyspaceName")
    public static Object[][] newKeyspaceName() {
        return new Object[][]{ { "lowercase2" }, { "\"CaseSensitive2\"" } };
    }

    @AfterMethod
    public void resetListeners(){
        for (SchemaChangeTestListener listener : listeners) {
            listener.reset(); // reset event counter
            reset(listener); // reset Mockito spy
        }
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_creation(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace, TABLE_ADDED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNotNull();
        for (SchemaChangeTestListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableAdded(added.capture());
            TableMetadata actual = added.getValue();
            assertThat(actual.getKeyspace().getName()).isEqualTo(handleId(keyspace));
            assertThat(actual.getName()).isEqualTo("table1");
        }
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_update(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace, TABLE_ADDED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1").getColumn("j")).isNull();
        execute(ALTER_TABLE, keyspace, TABLE_UPDATED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1").getColumn("j")).isNotNull();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableAdded(added.capture());
            assertThat(added.getValue().getKeyspace().getName()).isEqualTo(handleId(keyspace));
            assertThat(added.getValue().getName()).isEqualTo("table1");
            ArgumentCaptor<TableMetadata> current = ArgumentCaptor.forClass(TableMetadata.class);
            ArgumentCaptor<TableMetadata> previous = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableChanged(current.capture(), previous.capture());
            assertThat(previous.getValue()).isEqualTo(added.getValue());
            assertThat(previous.getValue().getColumn("j")).isNull();
            assertThat(current.getValue().getKeyspace().getName()).isEqualTo(handleId(keyspace));
            assertThat(current.getValue().getName()).isEqualTo("table1");
            assertThat(current.getValue().getColumn("j")).isNotNull();
        }
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_drop(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace, TABLE_ADDED);
        execute(DROP_TABLE, keyspace, TABLE_REMOVED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNull();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableAdded(added.capture());
            assertThat(added.getValue().getKeyspace().getName()).isEqualTo(handleId(keyspace));
            assertThat(added.getValue().getName()).isEqualTo("table1");
            ArgumentCaptor<TableMetadata> removed = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableRemoved(removed.capture());
            assertThat(removed.getValue().getName()).isEqualTo("table1");
        }
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_creation(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace, KS_ADDED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace)).isNotNull();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, atLeastOnce()).onKeyspaceAdded(added.capture());
            assertThat(added.getValue().getName()).isEqualTo(handleId(keyspace));
        }
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_update(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace, KS_ADDED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).isDurableWrites()).isTrue();
        execute(ALTER_KEYSPACE, keyspace, KS_UPDATED);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).isDurableWrites()).isFalse();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, atLeastOnce()).onKeyspaceAdded(added.capture());
            assertThat(added.getValue().getName()).isEqualTo(handleId(keyspace));
            ArgumentCaptor<KeyspaceMetadata> current = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            ArgumentCaptor<KeyspaceMetadata> previous = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, atLeastOnce()).onKeyspaceChanged(current.capture(), previous.capture());
            assertThat(previous.getValue()).isEqualTo(added.getValue());
            assertThat(previous.getValue().isDurableWrites()).isTrue();
            assertThat(current.getValue().getName()).isEqualTo(handleId(keyspace));
            assertThat(current.getValue().isDurableWrites()).isFalse();
        }
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_drop(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace, KS_ADDED);
        //        Thread.sleep(5000);
        for (Metadata m : metadatas)
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isNotEmpty();
        execute(CREATE_TABLE, keyspace, TABLE_ADDED); // to test table drop notifications
        execute(DROP_KEYSPACE, keyspace, KS_REMOVED);
        for (Metadata m : metadatas) {
            assertThat(m.getKeyspace(keyspace)).isNull();
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isEmpty();
        }
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> table = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, atLeastOnce()).onTableRemoved(table.capture());
            assertThat(table.getValue().getName()).isEqualTo("table1");
            assertThat(table.getValue().getKeyspace().getName()).isEqualTo(handleId(keyspace));
            ArgumentCaptor<KeyspaceMetadata> ks = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, atLeastOnce()).onKeyspaceRemoved(ks.capture());
            assertThat(ks.getValue().getName()).isEqualTo(handleId(keyspace));
        }
    }

    @AfterMethod(groups = "short")
    public void cleanup() throws InterruptedException {
        ListenableFuture<List<ResultSet>> f = Futures.successfulAsList(Lists.newArrayList(
            session.executeAsync("DROP TABLE lowercase.table1"),
            session.executeAsync("DROP TABLE \"CaseSensitive\".table1"),
            session.executeAsync("DROP KEYSPACE lowercase2"),
            session.executeAsync("DROP KEYSPACE \"CaseSensitive2\"")
        ));
        Futures.getUnchecked(f);
    }

    private void execute(String cql, String keyspace, EventType eventType) throws InterruptedException {
        session.execute(String.format(cql, keyspace));
        for (SchemaChangeTestListener listener : listeners)
            listener.await(eventType, 1);
    }

    enum EventType {
        KS_ADDED, KS_REMOVED, KS_UPDATED,
        TABLE_ADDED, TABLE_REMOVED, TABLE_UPDATED
    }

    class SchemaChangeTestListener implements SchemaChangeListener {

        final Map<EventType, AtomicInteger> counters = new ConcurrentHashMap<EventType, AtomicInteger>();

        final Lock lock = new ReentrantLock();

        final Condition cond = lock.newCondition();

        SchemaChangeTestListener() {
            reset();
        }

        void reset() {
            for (EventType eventType : EventType.values()) {
                counters.put(eventType, new AtomicInteger(0));
            }
        }

        void await(EventType eventType, int events) throws InterruptedException {
            long nanos = SECONDS.toNanos(10);
            lock.lock();
            try {
                AtomicInteger counter = counters.get(eventType);
                while (counter.get() < events) {
                    if (nanos <= 0L) break; // timeout
                    nanos = cond.awaitNanos(nanos);
                }
            } finally {
                lock.unlock();
            }
        }

        void eventReceived(EventType eventType) {
            lock.lock();
            try {
                counters.get(eventType).incrementAndGet();
                cond.signal();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void onKeyspaceAdded(KeyspaceMetadata keyspace) {
            eventReceived(KS_ADDED);
        }

        @Override
        public void onKeyspaceRemoved(KeyspaceMetadata keyspace) {
            eventReceived(EventType.KS_REMOVED);
        }

        @Override
        public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {
            eventReceived(EventType.KS_UPDATED);
        }

        @Override
        public void onTableAdded(TableMetadata table) {
            eventReceived(EventType.TABLE_ADDED);
        }

        @Override
        public void onTableRemoved(TableMetadata table) {
            eventReceived(EventType.TABLE_REMOVED);
        }

        @Override
        public void onTableChanged(TableMetadata current, TableMetadata previous) {
            eventReceived(EventType.TABLE_UPDATED);
        }

    }
}

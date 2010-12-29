/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.WrappedRunnable;
import  org.apache.thrift.TException;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MutationTest extends TestBase
{
    @Test
    public void testInsert() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        client.set_keyspace(KEYSPACE);

        String rawKey = String.format("test.key.%d", System.currentTimeMillis());
        ByteBuffer key = ByteBuffer.wrap(rawKey.getBytes());

        ColumnParent     cp = new ColumnParent("Standard1");
        Column col1 = new Column(
            ByteBuffer.wrap("c1".getBytes()),
            ByteBuffer.wrap("v1".getBytes()),
            0
            );
        insert(client, key, cp, col1, ConsistencyLevel.ONE);
        Column col2 = new Column(
            ByteBuffer.wrap("c2".getBytes()),
            ByteBuffer.wrap("v2".getBytes()),
            0
            );
        insert(client, key, cp, col2, ConsistencyLevel.ONE);

        Thread.sleep(100);

        // verify get
        assertEquals(
            getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE),
            col1
            );

        List<ColumnOrSuperColumn> coscs = new LinkedList<ColumnOrSuperColumn>();
        coscs.add((new ColumnOrSuperColumn()).setColumn(col1));
        coscs.add((new ColumnOrSuperColumn()).setColumn(col2));
        assertEquals(
            get_slice(client, key, cp, ConsistencyLevel.ONE),
            coscs
            );
    }

    public void insert(Cassandra.Client client, ByteBuffer key, ColumnParent cp, Column col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        client.insert(key, cp, col, cl);
    }

    public Column getColumn(Cassandra.Client client, ByteBuffer key, String cf, String col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException
    {
        ColumnPath cpath = new ColumnPath(cf);
        cpath.setColumn(col.getBytes());
        return client.get(key, cpath, cl).column;
    }

    public List<ColumnOrSuperColumn> get_slice(Cassandra.Client client, ByteBuffer key, ColumnParent cp, ConsistencyLevel cl)
      throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        // verify slice
        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(
            new SliceRange(
                ByteBuffer.wrap(new byte[0]),
                ByteBuffer.wrap(new byte[0]),
                false,
                1000
                )
            );
        return client.get_slice(key, cp, sp, cl);
    }

    @Test
    public void testQuorumInsertThenFailure() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        client.set_keyspace(KEYSPACE);

        String rawKey = String.format("test.key.%d", System.currentTimeMillis());
        ByteBuffer key = ByteBuffer.wrap(rawKey.getBytes());

        ColumnParent     cp = new ColumnParent("Standard1");
        Column col1 = new Column(
            ByteBuffer.wrap("c1".getBytes()),
            ByteBuffer.wrap("v1".getBytes()),
            0
            );
        client.insert(key, cp, col1, ConsistencyLevel.QUORUM);
        Column col2 = new Column(
            ByteBuffer.wrap("c2".getBytes()),
            ByteBuffer.wrap("v2".getBytes()),
            0
            );
        client.insert(key, cp, col2, ConsistencyLevel.QUORUM);

        Thread.sleep(100);

        Failure failure = controller.failHosts(hosts.get(0));
        try
        {
            // our original client connection is dead: open a new one
            client = controller.createClient(hosts.get(1));
            client.set_keyspace(KEYSPACE);

            // verify get
            assertEquals(
                getColumn(client, key, "Standard1", "c1", ConsistencyLevel.QUORUM),
                col1
                );

            // verify slice
            List<ColumnOrSuperColumn> coscs = new LinkedList<ColumnOrSuperColumn>();
            coscs.add((new ColumnOrSuperColumn()).setColumn(col1));
            coscs.add((new ColumnOrSuperColumn()).setColumn(col2));
            assertEquals(
                get_slice(client, key, cp, ConsistencyLevel.QUORUM),
                coscs
                );
        }
        finally
        {
            failure.resolve();
        }
    }
}

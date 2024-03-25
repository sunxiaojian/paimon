/*
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

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.RowDataPartitionComputer;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** {@link MetastoreClient} for Hive tables. */
public class HiveMetastoreClient implements MetastoreClient {

    private final Identifier identifier;
    private final RowDataPartitionComputer partitionComputer;

    private final ClientPool<IMetaStoreClient, TException> clients;
    private final StorageDescriptor sd;

    private HiveMetastoreClient(
            Identifier identifier,
            TableSchema schema,
            ClientPool<IMetaStoreClient, TException> clients)
            throws Exception {
        this.identifier = identifier;
        this.partitionComputer =
                new RowDataPartitionComputer(
                        new CoreOptions(schema.options()).partitionDefaultName(),
                        schema.logicalPartitionType(),
                        schema.partitionKeys().toArray(new String[0]));

        this.clients = clients;
        this.sd =
                clients.run(
                        client ->
                                client.getTable(
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .getSd());
    }

    @Override
    public void addPartition(BinaryRow partition) throws Exception {
        addPartition(partitionComputer.generatePartValues(partition));
    }

    @Override
    public void addPartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            clients.run(
                    client ->
                            client.getPartition(
                                    identifier.getDatabaseName(),
                                    identifier.getObjectName(),
                                    partitionValues));
            // do nothing if the partition already exists
        } catch (NoSuchObjectException e) {
            // partition not found, create new partition
            StorageDescriptor newSd = new StorageDescriptor(sd);
            newSd.setLocation(
                    sd.getLocation()
                            + "/"
                            + PartitionPathUtils.generatePartitionPath(partitionSpec));

            Partition hivePartition = new Partition();
            hivePartition.setDbName(identifier.getDatabaseName());
            hivePartition.setTableName(identifier.getObjectName());
            hivePartition.setValues(partitionValues);
            hivePartition.setSd(newSd);
            int currentTime = (int) (System.currentTimeMillis() / 1000);
            hivePartition.setCreateTime(currentTime);
            hivePartition.setLastAccessTime(currentTime);

            clients.run(client -> client.add_partition(hivePartition));
        }
    }

    @Override
    public void deletePartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            clients.run(
                    client ->
                            client.dropPartition(
                                    identifier.getDatabaseName(),
                                    identifier.getObjectName(),
                                    partitionValues,
                                    false));
        } catch (NoSuchObjectException e) {
            // do nothing if the partition not exists
        }
    }

    @Override
    public void close() throws Exception {
        // Do nothing
    }

    /** Factory to create {@link HiveMetastoreClient}. */
    public static class Factory implements MetastoreClient.Factory {

        private static final long serialVersionUID = 1L;

        private final SerializableHiveConf hiveConf;
        private final Identifier identifier;
        private final TableSchema schema;
        private final String clientClassName;
        private final Options options;

        public Factory(
                SerializableHiveConf hiveConf,
                Identifier identifier,
                TableSchema schema,
                String clientClassName,
                Options options) {
            this.hiveConf = hiveConf;
            this.identifier = identifier;
            this.schema = schema;
            this.clientClassName = clientClassName;
            this.options = options;
        }

        @Override
        public MetastoreClient create() {
            try {
                return new HiveMetastoreClient(
                        identifier,
                        schema,
                        HiveCatalog.createClients(
                                hiveConf.conf(),
                                clientClassName,
                                options,
                                options.get(CatalogOptions.CLIENT_POOL_SIZE)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

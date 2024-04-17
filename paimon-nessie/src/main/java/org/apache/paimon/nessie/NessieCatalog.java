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

package org.apache.paimon.nessie;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public class NessieCatalog extends AbstractCatalog {
    protected NessieCatalog(FileIO fileIO) {
        super(fileIO);
    }

    @Override
    protected boolean databaseExistsImpl(String databaseName) {
        return false;
    }

    @Override
    protected Map<String, String> loadDatabasePropertiesImpl(String name) {
        return null;
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {}

    @Override
    protected void dropDatabaseImpl(String name) {}

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return null;
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {}

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {}

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {}

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {}

    /**
     * Get the warehouse path for the catalog if exists.
     *
     * @return The catalog warehouse path.
     */
    @Override
    public String warehouse() {
        return null;
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        return null;
    }

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    @Override
    public List<String> listDatabases() {
        return null;
    }

    /**
     * Closes this resource, relinquishing any underlying resources. This method is invoked
     * automatically on objects managed by the {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code Exception}, implementers are
     * <em>strongly</em> encouraged to declare concrete implementations of the {@code close} method
     * to throw more specific exceptions, or to throw no exception at all if the close operation
     * cannot fail.
     *
     * <p>Cases where the close operation may fail require careful attention by implementers. It is
     * strongly advised to relinquish the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code close} method is unlikely to
     * be invoked more than once and so this ensures that the resources are released in a timely
     * manner. Furthermore it reduces problems that could arise when the resource wraps, or is
     * wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised to not have the {@code close}
     * method throw {@link InterruptedException}.</em>
     *
     * <p>This exception interacts with a thread's interrupted status, and runtime misbehavior is
     * likely to occur if an {@code InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     *
     * <p>More generally, if it would cause problems for an exception to be suppressed, the {@code
     * AutoCloseable.close} method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close} method of {@link Closeable}, this
     * {@code close} method is <em>not</em> required to be idempotent. In other words, calling this
     * {@code close} method more than once may have some visible side effect, unlike {@code
     * Closeable.close} which is required to have no effect if called more than once.
     *
     * <p>However, implementers of this interface are strongly encouraged to make their {@code
     * close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {}
}

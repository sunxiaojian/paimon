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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.branch.Branch;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.List;

/**
 * Delete branch procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.expire_branch('tableId')
 * </code></pre>
 */
public class ExpireBranchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "expire_branch";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
            })
    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        List<Branch> expireBranches =
                catalog.getTable(Identifier.fromString(tableId)).expireBranches();
        return expireBranches == null || expireBranches.isEmpty()
                ? new String[] {"No expired branches."}
                : expireBranches.stream().map(Branch::getBranchName).toArray(String[]::new);
    }
}
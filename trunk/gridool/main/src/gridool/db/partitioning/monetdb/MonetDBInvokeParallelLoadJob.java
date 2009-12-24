/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package gridool.db.partitioning.monetdb;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.db.DBTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

import xbird.util.primitive.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBInvokeParallelLoadJob extends
        GridJobBase<Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>>, Long> {
    private static final long serialVersionUID = 3356783271521697124L;

    private transient long numProcessed = 0L;

    public MonetDBInvokeParallelLoadJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>> args)
            throws GridException {
        final MonetDBParallelLoadOperation ops = args.getFirst();
        final String driverClassName = ops.getDriverClassName();
        final String connectUrl = ops.getConnectUrl();
        final String userName = ops.getUserName();
        final String password = ops.getPassword();
        final String tableName = ops.getTableName();
        final String createTableDDL = ops.getCreateTableDDL();
        final String alterTableDDL = ops.getAlterTableDDL();

        final Map<GridNode, MutableInt> assigned = args.getSecond();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(assigned.size());
        for(final Map.Entry<GridNode, MutableInt> e : assigned.entrySet()) {
            GridNode node = e.getKey();
            int numRecords = e.getValue().intValue();
            String copyIntoQuery = (numRecords == 0) ? null : ops.getCopyIntoQuery(numRecords);
            MonetDBParallelLoadOperation shrinkedOps = new MonetDBParallelLoadOperation(driverClassName, connectUrl, tableName, createTableDDL, copyIntoQuery, alterTableDDL);
            shrinkedOps.setAuth(userName, password);
            GridTask task = new DBTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        Integer processed = result.getResult();
        if(processed != null) {
            numProcessed += processed.intValue();
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return numProcessed;
    }

}

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
package gridool.lib.db.monetdb;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.lib.db.DBTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

import xbird.util.datetime.StopWatch;
import xbird.util.primitives.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBInvokeCopyIntoJob extends
        GridJobBase<Pair<MonetDBInvokeCopyIntoOperation, Map<GridNode, MutableInt>>, Long> {
    private static final long serialVersionUID = 2379356861976547924L;

    private transient StopWatch sw;

    public MonetDBInvokeCopyIntoJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<MonetDBInvokeCopyIntoOperation, Map<GridNode, MutableInt>> args)
            throws GridException {
        this.sw = new StopWatch();

        final MonetDBInvokeCopyIntoOperation ops = args.first;
        final String driverClassName = ops.getDriverClassName();
        final String connectUrl = ops.getConnectUrl();
        final String tableName = ops.getTableName();
        final String userName = ops.getUserName();
        final String password = ops.getPassword();

        final Map<GridNode, MutableInt> assigned = args.second;
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(assigned.size());
        for(final Map.Entry<GridNode, MutableInt> e : assigned.entrySet()) {
            GridNode node = e.getKey();
            MutableInt numRecords = e.getValue();
            String query = ops.getCopyIntoQuery(numRecords.intValue());
            MonetDBInvokeCopyIntoOperation shrinkedOps = new MonetDBInvokeCopyIntoOperation(driverClassName, connectUrl, tableName, query);
            shrinkedOps.setAuth(userName, password);
            GridTask task = new DBTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return sw.elapsed();
    }

}

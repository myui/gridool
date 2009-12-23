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
package gridool.db;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.db.record.DBRecord;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBInsertRecordJob extends GridJobBase<DBInsertOperation, Serializable> {
    private static final long serialVersionUID = -8446997971270275539L;

    public DBInsertRecordJob() {}

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final DBInsertOperation ops)
            throws GridException {
        final DBRecord[] records = ops.getRecords();
        final int numNodes = router.getGridSize();
        final Map<GridNode, List<DBRecord>> nodeAssignMap = new HashMap<GridNode, List<DBRecord>>(numNodes);
        for(DBRecord rec : records) {
            byte[] key = rec.getKey();
            GridNode node = router.selectNode(key);
            if(node == null) {
                throw new GridException("Could not find any node in cluster.");
            }
            List<DBRecord> mappedRecords = nodeAssignMap.get(node);
            if(mappedRecords == null) {
                mappedRecords = new ArrayList<DBRecord>();
                nodeAssignMap.put(node, mappedRecords);
            }
            mappedRecords.add(rec);
        }

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        for(Map.Entry<GridNode, List<DBRecord>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            List<DBRecord> recordlist = e.getValue();
            DBRecord[] recordAry = new DBRecord[recordlist.size()];
            recordlist.toArray(recordAry);
            DBInsertOperation shrinkedOps = ops.makeOperation(recordAry);
            GridTask task = new DBTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Serializable reduce() throws GridException {
        return null;
    }

}

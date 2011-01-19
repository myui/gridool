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
import gridool.db.record.MultiKeyGenericDBRecord;
import gridool.routing.GridRouter;
import gridool.util.collections.IdentityHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DBInsertMultiKeyRecordJob extends GridJobBase<DBInsertOperation, Float> {
    private static final long serialVersionUID = -8446997971270275539L;

    private transient long overlappingRecords = 0L;
    private transient long totalShuffledRecords = 0L;

    public DBInsertMultiKeyRecordJob() {}

    public Map<GridTask, GridNode> map(final GridRouter router, final DBInsertOperation ops)
            throws GridException {
        final MultiKeyGenericDBRecord[] records = ops.getRecords();
        final int numNodes = router.getGridSize();
        final Map<GridNode, List<DBRecord>> nodeAssignMap = new HashMap<GridNode, List<DBRecord>>(numNodes);
        final Set<GridNode> mappedNodes = new IdentityHashSet<GridNode>();
        int overlaps = 0;
        for(MultiKeyGenericDBRecord rec : records) {
            final byte[][] keys = rec.getKeys();
            boolean hasOverlap = false;
            for(byte[] key : keys) {
                final GridNode node = router.selectNode(key);
                if(node == null) {
                    throw new GridException("Could not find any node in cluster.");
                }
                if(mappedNodes.add(node)) {// insert record if the given record is not associated yet.
                    List<DBRecord> mappedRecords = nodeAssignMap.get(node);
                    if(mappedRecords == null) {
                        mappedRecords = new ArrayList<DBRecord>();
                        nodeAssignMap.put(node, mappedRecords);
                    }
                    mappedRecords.add(rec);
                } else {
                    hasOverlap = true;
                }
            }
            if(hasOverlap) {
                overlaps++;
            }
            mappedNodes.clear();
        }
        this.overlappingRecords = overlaps;
        this.totalShuffledRecords = records.length;

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

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Float reduce() throws GridException {
        return new Float(overlappingRecords / (double) totalShuffledRecords);
    }

}

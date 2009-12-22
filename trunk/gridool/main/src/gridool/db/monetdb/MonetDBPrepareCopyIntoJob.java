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
package gridool.db.monetdb;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.db.DBInsertOperation;
import gridool.db.DBTaskAdapter;
import gridool.db.MultiKeyRowPlaceholderRecord;
import gridool.routing.GridTaskRouter;

import java.nio.charset.Charset;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import xbird.util.collections.IdentityHashSet;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.primitive.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBPrepareCopyIntoJob extends
        GridJobBase<DBInsertOperation, Map<GridNode, MutableInt>> {
    private static final long serialVersionUID = -8446997971270275539L;

    private transient Map<GridNode, MutableInt> assignedRecMap;

    public MonetDBPrepareCopyIntoJob() {}

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final DBInsertOperation ops)
            throws GridException {
        final MultiKeyRowPlaceholderRecord[] records = ops.getRecords();
        assert (records != null && records.length > 0);

        final int numNodes = router.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new IdentityHashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Set<GridNode> mappedNodes = new IdentityHashSet<GridNode>();
        final Charset charset = Charset.forName("UTF-8");
        final int totalRecords = records.length;
        for(int i = 0; i < totalRecords; i++) {
            MultiKeyRowPlaceholderRecord rec = records[i];
            records[i] = null;
            final byte[][] keys = rec.getKeys();
            for(final byte[] key : keys) {
                final GridNode node = router.selectNode(key);
                if(node == null) {
                    throw new GridException("Could not find any node in cluster.");
                }
                if(mappedNodes.add(node)) {// insert record if the given record is not associated yet.
                    final String row = rec.getRow();
                    final byte[] b = row.getBytes(charset);
                    final int blen = b.length;
                    final FastByteArrayOutputStream rowsBuf;
                    final Pair<MutableInt, FastByteArrayOutputStream> pair = nodeAssignMap.get(node);
                    if(pair == null) {
                        int expected = (blen * (totalRecords / numNodes)) << 1;
                        rowsBuf = new FastByteArrayOutputStream(Math.max(expected, 32786));
                        Pair<MutableInt, FastByteArrayOutputStream> newPair = new Pair<MutableInt, FastByteArrayOutputStream>(new MutableInt(1), rowsBuf);
                        nodeAssignMap.put(node, newPair);
                    } else {
                        rowsBuf = pair.second;
                        MutableInt cnt = pair.first;
                        cnt.increment();
                    }
                    rowsBuf.write(b, 0, blen);
                }
            }
            mappedNodes.clear();
        }

        final String driverClassName = ops.getDriverClassName();
        final String connectUrl = ops.getConnectUrl();
        final String createTableDDL = ops.getCreateTableDDL();
        final String tableName = ops.getTableName();
        final String userName = ops.getUserName();
        final String password = ops.getPassword();

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        final Map<GridNode, MutableInt> assignedRecMap = new IdentityHashMap<GridNode, MutableInt>(numNodes);
        for(final Map.Entry<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            Pair<MutableInt, FastByteArrayOutputStream> pair = e.getValue();
            MutableInt numRecords = pair.first;
            assignedRecMap.put(node, numRecords);
            FastByteArrayOutputStream rows = pair.second;
            byte[] b = rows.toByteArray();
            pair.clear();
            MonetDBPrepareCopyIntoOperation shrinkedOps = new MonetDBPrepareCopyIntoOperation(driverClassName, connectUrl, createTableDDL, tableName, b);
            shrinkedOps.setAuth(userName, password);
            GridTask task = new DBTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }
        this.assignedRecMap = assignedRecMap;
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

}

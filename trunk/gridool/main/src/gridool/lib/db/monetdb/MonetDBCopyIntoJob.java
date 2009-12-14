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
import gridool.lib.db.DBInsertOperation;
import gridool.lib.db.DBTaskAdapter;
import gridool.lib.db.MultiKeyRowPlaceholderRecord;
import gridool.routing.GridTaskRouter;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import xbird.util.collections.IdentityHashSet;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBCopyIntoJob extends GridJobBase<DBInsertOperation, Float> {
    private static final long serialVersionUID = -8446997971270275539L;

    private transient long overlappingRecords = 0L;
    private transient long totalShuffledRecords = 0L;

    public MonetDBCopyIntoJob() {}

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final DBInsertOperation ops)
            throws GridException {
        final MultiKeyRowPlaceholderRecord[] records = ops.getRecords();
        final int numNodes = router.getGridSize();
        final Map<GridNode, FastByteArrayOutputStream> nodeAssignMap = new HashMap<GridNode, FastByteArrayOutputStream>(numNodes);
        final Set<GridNode> mappedNodes = new IdentityHashSet<GridNode>();
        int overlaps = 0;
        for(MultiKeyRowPlaceholderRecord rec : records) {
            final byte[][] keys = rec.getKeys();
            boolean hasOverlap = false;
            for(byte[] key : keys) {
                final GridNode node = router.selectNode(key);
                if(node == null) {
                    throw new GridException("Could not find any node in cluster.");
                }
                if(mappedNodes.add(node)) {// insert record if the given record is not associated yet.
                    FastByteArrayOutputStream rowsBuf = nodeAssignMap.get(node);
                    if(rowsBuf == null) {
                        rowsBuf = new FastByteArrayOutputStream(16384);
                        nodeAssignMap.put(node, rowsBuf);
                    }
                    String row = rec.getRow();
                    byte[] b = StringUtils.getBytes(row);
                    rowsBuf.write(b, 0, b.length);
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
        {
            final String driverClassName = ops.getDriverClassName();
            final String connectUrl = ops.getConnectUrl();
            final String createTableDDL = ops.getCreateTableDDL();
            final String tableName = ops.getTableName();
            final String userName = ops.getUserName();
            final String password = ops.getPassword();

            final MultiKeyRowPlaceholderRecord rec = records[0];
            final String copyIntoQuery = "COPY INTO '" + tableName
                    + "' FROM '<src>' USING DELIMITERS '" + rec.getFieldSeparator() + "', '"
                    + rec.getRecordSeparator() + "', '" + rec.getStringQuote() + "' NULL AS '"
                    + rec.getNullString() + '\'';

            for(Map.Entry<GridNode, FastByteArrayOutputStream> e : nodeAssignMap.entrySet()) {
                GridNode node = e.getKey();
                FastByteArrayOutputStream rows = e.getValue();
                byte[] b = rows.toByteArray();
                MonetDBCopyIntoOperation shrinkedOps = new MonetDBCopyIntoOperation(driverClassName, connectUrl, createTableDDL, tableName, copyIntoQuery, b);
                shrinkedOps.setAuth(userName, password);
                GridTask task = new DBTaskAdapter(this, shrinkedOps);
                map.put(task, node);
            }
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Float reduce() throws GridException {
        return new Float(overlappingRecords / (double) totalShuffledRecords);
    }

}

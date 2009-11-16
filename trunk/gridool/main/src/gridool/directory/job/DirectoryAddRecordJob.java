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
package gridool.directory.job;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.directory.helpers.DirectoryTaskAdapter;
import gridool.directory.ops.AddOperation;
import gridool.mapred.db.DBRecord;
import gridool.routing.GridTaskRouter;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import xbird.util.collections.ArrayQueue;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class DirectoryAddRecordJob extends
        GridJobBase<Pair<String, ArrayQueue<DBRecord>>, Serializable> {
    private static final long serialVersionUID = -1739585555790175133L;

    public DirectoryAddRecordJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<String, ArrayQueue<DBRecord>> ops)
            throws GridException {
        final String idxName = ops.getFirst();
        final ArrayQueue<DBRecord> records = ops.getSecond();

        final int totalNodes = router.getGridSize();
        final Map<GridNode, List<DBRecord>> nodeAssignMap = new HashMap<GridNode, List<DBRecord>>(totalNodes);
        final int numRecords = records.size();
        for(int i = 0; i < numRecords; i++) {
            DBRecord record = records.get(i);
            byte[] key = record.getKey();
            GridNode node = router.selectNode(key);
            List<DBRecord> mappedKeys = nodeAssignMap.get(node);
            if(mappedKeys == null) {
                mappedKeys = new ArrayList<DBRecord>();
                nodeAssignMap.put(node, mappedKeys);
            }
            mappedKeys.add(record);
        }

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(totalNodes);
        FastByteArrayOutputStream buf = new FastByteArrayOutputStream(512);
        final DataOutput out = new DataOutputStream(buf);
        for(Map.Entry<GridNode, List<DBRecord>> entry : nodeAssignMap.entrySet()) {
            final List<DBRecord> mapped = entry.getValue();
            final int size = mapped.size();
            final byte[][] keys = new byte[size][];
            final byte[][] values = new byte[size][];
            for(int i = 0; i < size; i++) {
                final DBRecord r = mapped.get(i);
                final byte[] k = r.getKey();
                try {
                    r.writeFields(out);
                } catch (IOException ioe) {
                    throw new GridException(ioe);
                }
                byte[] v = buf.toByteArray();
                buf.clear();
                keys[i] = k;
                values[i] = v;
            }
            GridNode node = entry.getKey();
            AddOperation shrinkedOps = new AddOperation(idxName, keys, values);
            GridTask task = new DirectoryTaskAdapter(this, shrinkedOps);
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

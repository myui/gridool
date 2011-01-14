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
import gridool.db.record.DBRecord;
import gridool.directory.helpers.DirectoryTaskAdapter;
import gridool.directory.ops.AddOperation;
import gridool.marshaller.GridMarshaller;
import gridool.routing.GridRouter;
import gridool.util.collections.ArrayQueue;
import gridool.util.io.FastByteArrayOutputStream;

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
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class DirectoryAddRecordJob extends
        GridJobBase<DirectoryAddRecordJob.AddRecordOps, Serializable> {
    private static final long serialVersionUID = -1739585555790175133L;

    public DirectoryAddRecordJob() {}

    @SuppressWarnings("unchecked")
    public Map<GridTask, GridNode> map(GridRouter router, AddRecordOps ops)
            throws GridException {
        final String idxName = ops.idxName;
        final ArrayQueue<DBRecord> records = ops.records;
        final GridMarshaller marshaller = ops.marshaller;

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
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(512);
        for(Map.Entry<GridNode, List<DBRecord>> entry : nodeAssignMap.entrySet()) {
            final List<DBRecord> mapped = entry.getValue();
            final int size = mapped.size();
            final byte[][] keys = new byte[size][];
            final byte[][] values = new byte[size][];
            for(int i = 0; i < size; i++) {
                final DBRecord r = mapped.get(i);
                final byte[] k = r.getKey();
                r.writeTo(marshaller, out);
                byte[] v = out.toByteArray();
                out.reset();
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

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Serializable reduce() throws GridException {
        return null;
    }

    public static final class AddRecordOps {

        final String idxName;
        final ArrayQueue<DBRecord> records;
        @SuppressWarnings("unchecked")
        final GridMarshaller marshaller;

        @SuppressWarnings("unchecked")
        public AddRecordOps(String idxName, ArrayQueue<DBRecord> records, GridMarshaller marshaller) {
            this.idxName = idxName;
            this.records = records;
            this.marshaller = marshaller;
        }

    }

}

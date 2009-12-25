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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.FileAppendTask;
import gridool.routing.GridTaskRouter;

import java.nio.charset.Charset;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import xbird.util.collections.FixedArrayList;
import xbird.util.collections.IdentityHashSet;
import xbird.util.csv.CsvUtils;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.primitive.MutableInt;
import xbird.util.string.StringUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class CsvHashPartitioningJob extends
        GridJobBase<Pair<String[], DBPartitioningJobConf>, Map<GridNode, MutableInt>> {
    private static final long serialVersionUID = 149683992715077498L;

    private transient Map<GridNode, MutableInt> assignedRecMap;

    public CsvHashPartitioningJob() {}

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final Pair<String[], DBPartitioningJobConf> ops)
            throws GridException {
        final String[] lines = ops.getFirst();
        DBPartitioningJobConf jobConf = ops.getSecond();
        Pair<int[], int[]> partitioningKeys = jobConf.partitionigKeyIndices();
        final int[] pkeyIndicies = partitioningKeys.getFirst();
        final int[] fkeyIndicies = partitioningKeys.getSecond();
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        final String[] fields = new String[Math.max(pkeyIndicies == null ? 0 : pkeyIndicies.length, fkeyIndicies == null ? 0
                : fkeyIndicies.length)];
        assert (fields.length > 0);
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);

        final Charset charset = Charset.forName("UTF-8");
        final int totalRecords = lines.length;
        final int numNodes = router.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new IdentityHashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Set<GridNode> mappedNodes = new IdentityHashSet<GridNode>(numNodes);
        for(int i = 0; i < totalRecords; i++) {
            String line = lines[i];
            lines[i] = null;
            if(pkeyIndicies != null) {
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                final StringBuilder pkeys = new StringBuilder(64);
                for(final String k : fields) {
                    if(k == null) {
                        break;
                    }
                    pkeys.append(k);
                }
                mapRecord(line, totalRecords, charset, router, nodeAssignMap, mappedNodes, pkeys.toString());
            }
            if(fkeyIndicies != null) {
                CsvUtils.retrieveFields(line, fkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                mapRecord(line, totalRecords, charset, router, nodeAssignMap, mappedNodes, fields);
            }
            mappedNodes.clear();
        }

        String tableName = jobConf.getTableName();
        final String fileName = tableName + ".csv";
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
            GridTask task = new FileAppendTask(this, fileName, b);
            map.put(task, node);
        }

        for(final GridNode node : router.getAllNodes()) {
            if(!assignedRecMap.containsKey(node)) {
                assignedRecMap.put(node, new MutableInt(0));
            }
        }

        this.assignedRecMap = assignedRecMap;
        return map;
    }

    private static void mapRecord(final String line, final int totalRecords, final Charset charset, final GridTaskRouter router, final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap, final Set<GridNode> mappedNodes, final String... fields)
            throws GridException {
        final int numNodes = router.getGridSize();
        for(final String f : fields) {
            if(f == null) {
                break;
            }
            final byte[] k = StringUtils.getBytes(f);
            final GridNode node = router.selectNode(k);
            if(node == null) {
                throw new GridException("Could not find any node in cluster.");
            }
            if(mappedNodes.add(node)) {// insert record if the given record is not associated yet.
                final byte[] b = line.getBytes(charset);
                final int blen = b.length;
                final FastByteArrayOutputStream rowsBuf;
                final Pair<MutableInt, FastByteArrayOutputStream> pair = nodeAssignMap.get(node);
                if(pair == null) {
                    int expected = (int) ((blen * (totalRecords / numNodes)) * 1.3f);
                    rowsBuf = new FastByteArrayOutputStream(Math.max(expected, 32786));
                    Pair<MutableInt, FastByteArrayOutputStream> newPair = new Pair<MutableInt, FastByteArrayOutputStream>(new MutableInt(1), rowsBuf);
                    nodeAssignMap.put(node, newPair);
                } else {
                    rowsBuf = pair.second;
                    MutableInt cnt = pair.first;
                    cnt.increment();
                }
                rowsBuf.write(b, 0, blen);
                rowsBuf.write('\n'); // TODO FIXME support other record separator 
            }
        }
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

}

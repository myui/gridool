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
package gridool.db.partitioning.phihash.csv.dist;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.phihash.DBPartitioningJobConf;
import gridool.db.partitioning.phihash.csv.PartitioningJobConf;
import gridool.routing.GridRouter;
import gridool.util.collections.FixedArrayList;
import gridool.util.csv.CsvUtils;
import gridool.util.primitive.MutableInt;
import gridool.util.string.StringUtils;
import gridool.util.struct.Pair;

import java.util.ArrayList;
import java.util.Collection;
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
public final class GlobalCsvHashPartitioningJob extends
        GridJobBase<PartitioningJobConf, Map<GridNode, MutableInt>> {
    private static final long serialVersionUID = 149683992715077498L;

    private transient Map<GridNode, MutableInt> assignedRecMap;

    public GlobalCsvHashPartitioningJob() {}

    @Override
    public boolean handleNodeFailure() {
        return false;
    }

    public Map<GridTask, GridNode> map(final GridRouter router, final PartitioningJobConf ops)
            throws GridException {
        // COPY INTO control resources 
        DBPartitioningJobConf jobConf = ops.getJobConf();
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();

        // working resources
        Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys = ops.getPrimaryForeignKeys();
        PrimaryKey primaryKey = primaryForeignKeys.getFirst();
        final int[] pkeyIndicies = primaryKey.getColumnPositions(true);
        final String[] fields = new String[pkeyIndicies.length];
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);
        final StringBuilder strBuf = new StringBuilder(64);
        final String[] lines = ops.getLines();
        final int totalRecords = lines.length;
        final int numNodes = router.getGridSize();
        final int recordsPerNode = (int) ((totalRecords / numNodes) * 1.2);
        final Map<GridNode, List<String>> nodeAssignMap = new HashMap<GridNode, List<String>>(numNodes);
        for(int i = 0; i < totalRecords; i++) {
            String line = lines[i];
            lines[i] = null;
            CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
            fieldList.trimToZero();
            String pkeysField = GridDbUtils.combineFields(fields, pkeyIndicies.length, strBuf);
            // "primary" fragment mapping
            byte[] distkey = StringUtils.getBytes(pkeysField);
            GridNode mappedNode = router.selectNode(distkey);
            if(mappedNode == null) {
                throw new GridException("Could not find any node in cluster.");
            }
            List<String> list = nodeAssignMap.get(mappedNode);
            if(list == null) {
                list = new ArrayList<String>(recordsPerNode);
                nodeAssignMap.put(mappedNode, list);
            }
            list.add(line);
        }

        final int numTasks = nodeAssignMap.size();
        final Map<GridTask, GridNode> taskMap = new IdentityHashMap<GridTask, GridNode>(numTasks);
        for(final Map.Entry<GridNode, List<String>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            List<String> lineList = e.getValue();
            GridTask task = new InvokeLocalCsvPartitioningTask(this, lineList, ops, router);
            taskMap.put(task, node);
        }

        this.assignedRecMap = new HashMap<GridNode, MutableInt>(numNodes);
        return taskMap;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final HashMap<GridNode, MutableInt> map = result.getResult();
        if(map == null) {
            GridNode node = result.getExecutedNode();
            String errmsg = getClass().getSimpleName() + " failed on node: " + node;
            GridException err = result.getException();
            throw new GridException(errmsg, err);
        }
        for(final Map.Entry<GridNode, MutableInt> e : map.entrySet()) {
            GridNode node = e.getKey();
            MutableInt count = e.getValue();
            MutableInt prevCount = assignedRecMap.get(node);
            if(prevCount == null) {
                assignedRecMap.put(node, count);
            } else {
                int v = count.intValue();
                prevCount.add(v);
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

}

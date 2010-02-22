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
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.communication.payload.GridNodeInfo;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.catalog.PartitionKey;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.FileAppendTask;
import gridool.directory.ILocalDirectory;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.util.collections.FixedArrayList;
import xbird.util.collections.IdentityHashSet;
import xbird.util.csv.CsvUtils;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.io.IOUtils;
import xbird.util.primitive.MutableInt;
import xbird.util.string.StringUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CsvHashPartitioningJob extends
        GridJobBase<CsvHashPartitioningJob.JobConf, Map<GridNode, MutableInt>> {
    private static final long serialVersionUID = 149683992715077498L;
    private static final Log LOG = LogFactory.getLog(CsvHashPartitioningJob.class);

    private transient Map<GridNode, MutableInt> assignedRecMap;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public CsvHashPartitioningJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final CsvHashPartitioningJob.JobConf ops)
            throws GridException {
        assert (registry != null);
        final String[] lines = ops.getLines();
        final String csvFileName = ops.getFileName();
        final boolean append = !ops.isFirst();
        final DBPartitioningJobConf jobConf = ops.getJobConf();

        // partitioning resources
        final String actualTableName;
        final PartitionKey primaryPartitionKey;
        final List<PartitionKey> foreignPartitionKeys;
        final int pkeyPartitionNo;
        final int[] pkeyIndicies;
        final String[] parentTableFkIndexNames;
        {
            DistributionCatalog catalog = registry.getDistributionCatalog();
            actualTableName = jobConf.getTableName();
            String baseTableName = jobConf.getBaseTableName();
            Pair<PartitionKey, List<PartitionKey>> partitioningKeys = catalog.bindPartitioningKeyPositions(baseTableName, actualTableName);
            primaryPartitionKey = partitioningKeys.getFirst();
            foreignPartitionKeys = partitioningKeys.getSecond();
            pkeyPartitionNo = primaryPartitionKey.getPartitionNo();
            PrimaryKey primaryKey = primaryPartitionKey.getKey();
            pkeyIndicies = primaryKey.getColumnPositions(true);
            parentTableFkIndexNames = getParentTableFkIndexNames(primaryKey);
        }

        // COPY INTO control resources 
        final boolean insertHiddenField = jobConf.insertHiddenField();
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        // working resources
        final ILocalDirectory index = registry.getDirectory();
        final String[] fields = new String[getMaxColumnCount(primaryPartitionKey, foreignPartitionKeys)];
        assert (fields.length > 0);
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);
        final Charset charset = Charset.forName("UTF-8");
        final StringBuilder strBuf = new StringBuilder(64);
        final int totalRecords = lines.length;

        final int numNodes = router.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new IdentityHashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Map<GridNode, MutableInt> mappedNodes = new IdentityHashMap<GridNode, MutableInt>(numNodes);
        final int numDerived = foreignPartitionKeys.size();
        final GridNode[] derivedNodes = new GridNode[numDerived];
        final byte[][] fkKeys = new byte[numDerived][];
        final String[] fkIdxNames = getFkIndexNames(foreignPartitionKeys);
        final Set<GridNode> excludeNodeSet = new IdentityHashSet<GridNode>(numDerived);
        for(int i = 0; i < totalRecords; i++) {
            int bitShift = 0;
            String line = lines[i];
            lines[i] = null;
            final byte[] lineBytes = line.getBytes(charset);
            final GridNode pkMappedNode;
            {//primary mapping
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                String pkeysField = combineFields(fields, strBuf);
                byte[] distkey = StringUtils.getBytes(pkeysField);
                pkMappedNode = router.selectNode(distkey);
                MutableInt hiddenValue = decideRecordMapping(pkMappedNode, mappedNodes, pkeyPartitionNo);
                if(parentTableFkIndexNames != null) {
                    for(String idxName : parentTableFkIndexNames) {
                        mapBasedOnDrivedFragmentation(distkey, hiddenValue, mappedNodes, pkMappedNode, index, idxName);
                    }
                } else {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("No parent table with FK found for table: " + actualTableName);
                    }
                }
                bitShift++;
            }
            // derived mapping
            for(int j = 0; j < numDerived; j++) {
                PartitionKey partkey = foreignPartitionKeys.get(j);
                ForeignKey fk = partkey.getKey();
                int[] fkeyIndicies = fk.getFkColumnPositions(true);
                CsvUtils.retrieveFields(line, fkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                int partNo = partkey.getPartitionNo();
                String fkeysField = combineFields(fields, strBuf);
                byte[] distkey = StringUtils.getBytes(fkeysField);
                fkKeys[j] = distkey;
                GridNode node = router.selectNode(distkey);
                if(node == null) {
                    throw new GridException("Could not find any node in cluster.");
                }
                derivedNodes[j] = node;
                decideRecordMapping(node, mappedNodes, partNo);
            }
            if(mappedNodes.isEmpty()) {
                throw new IllegalStateException("Could not map records because there is neither PK nor FK in the template table of '"
                        + actualTableName + '\'');
            }
            for(int k = 0; k < numDerived; k++) {
                String fkIdxName = fkIdxNames[k];
                byte[] distkey = fkKeys[k];
                storeDerivedFragmentationInfo(distkey, derivedNodes, k, pkMappedNode, index, fkIdxName, excludeNodeSet);
            }
            mapRecord(lineBytes, totalRecords, numNodes, nodeAssignMap, mappedNodes, insertHiddenField, filedSeparator);
            mappedNodes.clear();
        }

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
            GridTask task = new FileAppendTask(this, csvFileName, b, append, true);
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

    private static MutableInt decideRecordMapping(final GridNode node, final Map<GridNode, MutableInt> mappedNodes, final int partitionNo)
            throws GridException {
        final MutableInt hiddenValue = mappedNodes.get(node);
        if(hiddenValue == null) {
            MutableInt newHidden = new MutableInt(partitionNo);
            mappedNodes.put(node, newHidden);
            return newHidden;
        }
        int newValue = hiddenValue.intValue() | partitionNo;
        hiddenValue.setValue(newValue);
        return hiddenValue;
    }

    private static void storeDerivedFragmentationInfo(final byte[] distkey, final GridNode[] derivedNodes, final int ith, final GridNode pkMappedNode, final ILocalDirectory index, final String idxName, final Set<GridNode> excludeNodeSet)
            throws GridException {
        excludeNodeSet.clear();

        final GridNode derivedNode = derivedNodes[ith];
        excludeNodeSet.add(derivedNode);
        if(pkMappedNode != derivedNode) {
            final byte[] v = pkMappedNode.toBytes();
            try {
                index.addMapping(idxName, distkey, v);
                excludeNodeSet.add(pkMappedNode);
            } catch (DbException e) {
                throw new GridException(e);
            }
        }
        for(int i = 0; i < derivedNodes.length; i++) {
            if(i == ith) {
                continue;
            }
            GridNode node = derivedNodes[i];
            if(excludeNodeSet.add(node)) {
                final byte[] v = node.toBytes();
                try {
                    index.addMapping(idxName, distkey, v);
                } catch (DbException e) {
                    throw new GridException(e);
                }
            }
        }
    }

    private static void mapBasedOnDrivedFragmentation(final byte[] distkey, final MutableInt hiddenValue, final Map<GridNode, MutableInt> mappedNodes, final GridNode pkNode, final ILocalDirectory index, final String parentTableFkIndex)
            throws GridException {
        final BTreeCallback handler = new BTreeCallback() {
            public boolean indexInfo(Value key, byte[] value) {
                GridNode node = GridNodeInfo.fromBytes(value);
                if(!node.equals(pkNode)) {
                    if(!mappedNodes.containsKey(node)) {
                        mappedNodes.put(node, hiddenValue);
                    }
                }
                return true;
            }

            public boolean indexInfo(Value value, long pointer) {
                throw new UnsupportedOperationException();
            }
        };
        try {
            index.exactSearch(parentTableFkIndex, distkey, handler);
        } catch (DbException e) {
            throw new GridException(e);
        }
    }

    private static void mapRecord(final byte[] line, final int totalRecords, final int numNodes, final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap, final Map<GridNode, MutableInt> mappedNodes, final boolean insertHiddenField, final char filedSeparator) {
        final int lineSize = line.length;
        for(Map.Entry<GridNode, MutableInt> e : mappedNodes.entrySet()) {
            final GridNode node = e.getKey();
            final int hiddenValue = e.getValue().intValue();

            final FastByteArrayOutputStream rowsBuf;
            final Pair<MutableInt, FastByteArrayOutputStream> pair = nodeAssignMap.get(node);
            if(pair == null) {
                int expected = (int) ((lineSize * (totalRecords / numNodes)) * 1.3f);
                rowsBuf = new FastByteArrayOutputStream(Math.max(expected, 32786));
                Pair<MutableInt, FastByteArrayOutputStream> newPair = new Pair<MutableInt, FastByteArrayOutputStream>(new MutableInt(1), rowsBuf);
                nodeAssignMap.put(node, newPair);
            } else {
                rowsBuf = pair.second;
                MutableInt cnt = pair.first;
                cnt.increment();
            }
            rowsBuf.write(line, 0, lineSize);
            if(insertHiddenField) {
                // FIXME workaround for MonetDB 
                final String str = Integer.toString(hiddenValue);
                final int strlen = str.length();
                for(int i = 0; i < strlen; i++) {
                    char c = str.charAt(i);
                    rowsBuf.write(c);
                }
                rowsBuf.write(filedSeparator);
            }
            rowsBuf.write('\n'); // TODO FIXME support other record separator 
        }
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

    static final class JobConf implements Externalizable {

        private String[] lines;
        private String fileName;
        private boolean isFirst;
        private DBPartitioningJobConf jobConf;

        public JobConf() {//for Externalizable
            super();
        }

        public JobConf(@Nonnull String[] lines, @Nonnull String fileName, boolean isFirst, @Nonnull DBPartitioningJobConf jobConf) {
            this.lines = lines;
            this.fileName = fileName;
            this.isFirst = isFirst;
            this.jobConf = jobConf;
        }

        String[] getLines() {
            return lines;
        }

        String getFileName() {
            return fileName;
        }

        boolean isFirst() {
            return isFirst;
        }

        DBPartitioningJobConf getJobConf() {
            return jobConf;
        }

        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            final int num = in.readInt();
            final String[] ary = new String[num];
            for(int i = 0; i < num; i++) {
                ary[i] = IOUtils.readString(in);
            }
            this.lines = ary;
            this.fileName = IOUtils.readString(in);
            this.isFirst = in.readBoolean();
            this.jobConf = (DBPartitioningJobConf) in.readObject();
        }

        public void writeExternal(final ObjectOutput out) throws IOException {
            final String[] ary = lines;
            final int num = ary.length;
            out.writeInt(num);
            for(int i = 0; i < num; i++) {
                IOUtils.writeString(ary[i], out);
            }
            IOUtils.writeString(fileName, out);
            out.writeBoolean(isFirst);
            out.writeObject(jobConf);
        }
    }

    private static int getMaxColumnCount(@Nonnull final PartitionKey primaryPartitionKey, @Nonnull final List<PartitionKey> foreignPartitionKeys) {
        int max = primaryPartitionKey.getKey().getColumnNames().size();
        for(final PartitionKey key : foreignPartitionKeys) {
            int size = key.getKey().getColumnNames().size();
            max = Math.max(size, max);
        }
        return max;
    }

    private static String combineFields(@Nonnull final String[] fields, @Nonnull final StringBuilder buf) {
        final int numFields = fields.length;
        if(numFields == 0) {
            throw new IllegalArgumentException();
        }
        if(numFields == 1) {
            return fields[0];
        }
        StringUtils.clear(buf);
        buf.append(fields[0]);
        for(int i = 1; i < numFields; i++) {
            buf.append('|');
            buf.append(fields[i]);
        }
        return buf.toString();
    }

    private static String[] getFkIndexNames(final List<PartitionKey> foreignPartitionKeys) {
        final int numKeys = foreignPartitionKeys.size();
        final String[] idxNames = new String[numKeys];
        for(int i = 0; i < numKeys; i++) {
            PartitionKey partKey = foreignPartitionKeys.get(i);
            ForeignKey fk = partKey.getKey();
            String fkTable = fk.getFkTableName();
            List<String> fkColumns = fk.getFkColumnNames();
            idxNames[i] = getIndexName(fkTable, fkColumns);
        }
        return idxNames;
    }

    private static String[] getParentTableFkIndexNames(final PrimaryKey childTablePkey) {
        final List<ForeignKey> parentFkeys = childTablePkey.getExportedKeys();
        if(parentFkeys == null) {
            return null;
        }
        final int numParents = parentFkeys.size();
        if(numParents == 0) {
            return null;
        }
        final String[] idxNames = new String[numParents];
        for(int i = 0; i < numParents; i++) {
            ForeignKey parentFkey = parentFkeys.get(i);
            String fkTable = parentFkey.getFkTableName();
            List<String> fkColumns = parentFkey.getFkColumnNames();
            String idxName = getIndexName(fkTable, fkColumns);
            idxNames[i] = idxName;
        }
        return idxNames;
    }

    private static String getIndexName(final String tableName, final List<String> columnNames) {
        final int numColumns = columnNames.size();
        if(numColumns == 0) {
            throw new IllegalArgumentException("No columns was specified for table: " + tableName);
        }
        final StringBuilder buf = new StringBuilder(32);
        buf.append(tableName);
        buf.append('.');
        for(int i = 0; i < numColumns; i++) {
            if(i != 0) {
                buf.append('_');
            }
            String colname = columnNames.get(i);
            buf.append(colname);
        }
        buf.append(".fktbl");
        return buf.toString();
    }

    private static GridNode getNodeFromIndex(final String idxName, final byte[] key, final ILocalDirectory index)
            throws GridException {
        final byte[] v;
        try {
            v = index.getValue(idxName, key);
        } catch (DbException e) {
            throw new GridException(e);
        }
        if(v == null) {
            return null;
        }
        GridNode node = GridNodeInfo.fromBytes(v);
        return node;
    }
}

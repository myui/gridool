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
import gridool.annotation.GridRegistryResource;
import gridool.communication.payload.GridNodeInfo;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.FileAppendTask;
import gridool.directory.ILocalDirectory;
import gridool.routing.GridTaskRouter;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.util.collections.FixedArrayList;
import xbird.util.collections.LRUMap;
import xbird.util.csv.CsvUtils;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.primitive.MutableInt;
import xbird.util.string.StringUtils;
import xbird.util.struct.Pair;

import com.sun.istack.internal.Nullable;

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
        final String tableName;
        final int tablePartitionNo;
        final PrimaryKey primaryKey;
        final Collection<ForeignKey> foreignKeys;
        final int[] pkeyIndicies;
        final String[] parentTableFkIndexNames;
        final int[] parentTablesPartitioNo;
        final int numParentForeignKeys;
        final boolean hasParentTable;
        final int numForeignKeys;
        final String[] fkIdxNames;
        final int[][] fkPositions;
        final LRUMap<String, List<GridNode>>[] fkCaches;
        {
            tableName = jobConf.getTableName();
            DistributionCatalog catalog = registry.getDistributionCatalog();
            int tableId = catalog.getTableId(tableName, true);
            tablePartitionNo = DistributionCatalog.getTablePartitionNo(tableId);
            Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys = ops.getPrimaryForeignKeys();
            primaryKey = primaryForeignKeys.getFirst();
            foreignKeys = primaryForeignKeys.getSecond();
            pkeyIndicies = primaryKey.getColumnPositions(true);
            parentTableFkIndexNames = getParentTableFkIndexNames(primaryKey);
            parentTablesPartitioNo = getParentTablesPartitionNo(primaryKey, catalog);
            hasParentTable = (parentTableFkIndexNames != null);
            numParentForeignKeys = hasParentTable ? parentTableFkIndexNames.length : 0;
            numForeignKeys = foreignKeys.size();
            fkIdxNames = getFkIndexNames(foreignKeys, numForeignKeys);
            fkPositions = getFkPositions(foreignKeys, numForeignKeys);
            fkCaches = getFkIndexCaches(numForeignKeys);
        }

        // COPY INTO control resources 
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        // working resources
        final ILocalDirectory index = registry.getDirectory();
        final String[] fields = new String[getMaxColumnCount(primaryKey, foreignKeys)];
        assert (fields.length > 0);
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);
        final Charset charset = Charset.forName("UTF-8");
        final StringBuilder strBuf = new StringBuilder(64);
        final int totalRecords = lines.length;

        final int numNodes = router.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new HashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Map<GridNode, MutableInt> mappedNodes = new HashMap<GridNode, MutableInt>(numNodes);
        for(int i = 0; i < totalRecords; i++) {
            String line = lines[i];
            lines[i] = null;
            final byte[] lineBytes = line.getBytes(charset);
            final GridNode pkMappedNode;
            {
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                String pkeysField = combineFields(fields, pkeyIndicies.length, strBuf);
                final byte[] distkey = StringUtils.getBytes(pkeysField);
                pkMappedNode = router.selectNode(distkey);
                // primary fragment mapping                
                mapPrimaryFragment(pkMappedNode, mappedNodes, tablePartitionNo);
                if(hasParentTable) {
                    // derived fragment mapping
                    assert (numParentForeignKeys != 0);
                    for(int kk = 0; kk < numParentForeignKeys; kk++) {
                        int partitionNo = parentTablesPartitioNo[kk];
                        String idxName = parentTableFkIndexNames[kk];
                        mapDrivedFragment(distkey, partitionNo, mappedNodes, index, idxName);
                    }
                }
                if(mappedNodes.isEmpty()) {
                    throw new IllegalStateException("Could not map records for table: '"
                            + tableName + '\'');
                }
                mapRecord(lineBytes, totalRecords, numNodes, nodeAssignMap, mappedNodes, filedSeparator);
                mappedNodes.clear();
            }
            {// store information for derived fragment mapping
                final byte[] mappedNodeBytes = pkMappedNode.toBytes();
                for(int jj = 0; jj < numForeignKeys; jj++) {
                    int[] pos = fkPositions[jj];
                    CsvUtils.retrieveFields(line, pos, fieldList, filedSeparator, quoteChar);
                    fieldList.trimToZero();
                    String fkeysField = combineFields(fields, pos.length, strBuf);
                    LRUMap<String, List<GridNode>> fkCache = fkCaches[jj];
                    List<GridNode> storedNodes = fkCache.get(fkeysField);
                    if(storedNodes == null) {
                        storedNodes = new ArrayList<GridNode>(8);
                        fkCache.put(fkeysField, storedNodes);
                        storedNodes.add(pkMappedNode);
                        storeDerivedFragmentationInfo(fkeysField, mappedNodeBytes, index, fkIdxNames[jj]);
                    } else {
                        if(!storedNodes.contains(pkMappedNode)) {
                            storedNodes.add(pkMappedNode);
                            storeDerivedFragmentationInfo(fkeysField, mappedNodeBytes, index, fkIdxNames[jj]);
                        }
                    }
                }
            }
        }

        final Map<GridTask, GridNode> taskmap = new IdentityHashMap<GridTask, GridNode>(numNodes);
        final Map<GridNode, MutableInt> assignedRecMap = new HashMap<GridNode, MutableInt>(numNodes);
        for(final Map.Entry<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            Pair<MutableInt, FastByteArrayOutputStream> pair = e.getValue();
            MutableInt numRecords = pair.first;
            assignedRecMap.put(node, numRecords);
            FastByteArrayOutputStream rows = pair.second;
            byte[] b = rows.toByteArray();
            pair.clear();
            GridTask task = new FileAppendTask(this, csvFileName, b, append, true);
            taskmap.put(task, node);
        }

        for(final GridNode node : router.getAllNodes()) {
            if(!assignedRecMap.containsKey(node)) {
                assignedRecMap.put(node, new MutableInt(0));
            }
        }
        this.assignedRecMap = assignedRecMap;
        return taskmap;
    }

    private static void mapPrimaryFragment(final GridNode node, final Map<GridNode, MutableInt> mappedNodes, final int partitionNo)
            throws GridException {
        assert (mappedNodes.isEmpty());
        if(node == null) {
            throw new GridException("Could not find any node in cluster.");
        }
        MutableInt newHidden = new MutableInt(partitionNo);
        mappedNodes.put(node, newHidden);
    }

    private static void mapDrivedFragment(final byte[] distkey, final int partitionNo, final Map<GridNode, MutableInt> mappedNodes, final ILocalDirectory index, final String parentTableFkIndex)
            throws GridException {
        final BTreeCallback handler = new BTreeCallback() {
            public boolean indexInfo(Value key, byte[] value) {
                GridNode node = GridNodeInfo.fromBytes(value);
                final MutableInt hiddenValue = mappedNodes.get(node);
                if(hiddenValue == null) {
                    MutableInt newHidden = new MutableInt(partitionNo);
                    mappedNodes.put(node, newHidden);
                } else {
                    final int oldValue = hiddenValue.intValue();
                    if(oldValue != partitionNo) {
                        int newValue = oldValue | partitionNo;
                        hiddenValue.setValue(newValue);
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

    private static void mapRecord(final byte[] line, final int totalRecords, final int numNodes, final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap, final Map<GridNode, MutableInt> mappedNodes, final char filedSeparator) {
        final int lineSize = line.length;
        for(Map.Entry<GridNode, MutableInt> e : mappedNodes.entrySet()) {
            final GridNode node = e.getKey();
            final int hiddenValue = e.getValue().intValue();

            final FastByteArrayOutputStream rowsBuf;
            final Pair<MutableInt, FastByteArrayOutputStream> pair = nodeAssignMap.get(node);
            if(pair == null) {
                int expected = (int) ((lineSize * (totalRecords / numNodes)) * 1.3f);
                if(expected > 209715200) {
                    LOG.warn("Expected record buffer for shuffling is too large: " + expected
                            + " bytes");
                }
                rowsBuf = new FastByteArrayOutputStream(Math.min(expected, 209715200)); //max 200MB
                Pair<MutableInt, FastByteArrayOutputStream> newPair = new Pair<MutableInt, FastByteArrayOutputStream>(new MutableInt(1), rowsBuf);
                nodeAssignMap.put(node, newPair);
            } else {
                rowsBuf = pair.second;
                MutableInt cnt = pair.first;
                cnt.increment();
            }
            rowsBuf.write(line, 0, lineSize);
            if(hiddenValue == 0) {
                throw new IllegalStateException("Illegal hidden value was detected");
            }
            final String str = Integer.toString(hiddenValue);
            final int strlen = str.length();
            for(int i = 0; i < strlen; i++) {
                char c = str.charAt(i);
                rowsBuf.write(c);
            }
            rowsBuf.write(filedSeparator); // REVIEWME this is monetdb workaround
            rowsBuf.write('\n'); // TODO FIXME support other record separator 
        }
    }

    private static void storeDerivedFragmentationInfo(final String fkeysField, final byte[] mappedNodeBytes, final ILocalDirectory index, final String idxName)
            throws GridException {
        final byte[] distkey = StringUtils.getBytes(fkeysField);
        try {
            index.addMapping(idxName, distkey, mappedNodeBytes);
        } catch (DbException e) {
            throw new GridException(e);
        }
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

    static final class JobConf {

        private final String[] lines;
        private final String fileName;
        private final boolean isFirst;
        private final Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
        private final DBPartitioningJobConf jobConf;

        public JobConf(@Nonnull String[] lines, @Nonnull String fileName, boolean isFirst, @Nonnull Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys, @Nonnull DBPartitioningJobConf jobConf) {
            this.lines = lines;
            this.fileName = fileName;
            this.isFirst = isFirst;
            this.primaryForeignKeys = primaryForeignKeys;
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

        Pair<PrimaryKey, Collection<ForeignKey>> getPrimaryForeignKeys() {
            return primaryForeignKeys;
        }

        DBPartitioningJobConf getJobConf() {
            return jobConf;
        }

    }

    private static int getMaxColumnCount(@Nonnull final PrimaryKey pkey, @Nonnull final Collection<ForeignKey> fkeys) {
        int max = pkey.getColumnNames().size();
        for(final ForeignKey fkey : fkeys) {
            int size = fkey.getColumnNames().size();
            max = Math.max(size, max);
        }
        return max;
    }

    private static String combineFields(@Nonnull final String[] fields, final int numFields, @Nonnull final StringBuilder buf) {
        if(numFields < 1) {
            throw new IllegalArgumentException("Illegal numField: " + numFields);
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

    private static String[] getFkIndexNames(final Collection<ForeignKey> fkeys, final int numFkeys) {
        final String[] idxNames = new String[numFkeys];
        final Iterator<ForeignKey> itor = fkeys.iterator();
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = itor.next();
            String fkTable = fk.getFkTableName();
            List<String> fkColumns = fk.getFkColumnNames();
            idxNames[i] = getIndexName(fkTable, fkColumns);
        }
        return idxNames;
    }

    private static int[][] getFkPositions(final Collection<ForeignKey> fkeys, final int numFkeys) {
        final int[][] positions = new int[numFkeys][];
        final Iterator<ForeignKey> itor = fkeys.iterator();
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = itor.next();
            int[] fkeyPos = fk.getFkColumnPositions(true);
            positions[i] = fkeyPos;
        }
        return positions;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static LRUMap<String, List<GridNode>>[] getFkIndexCaches(final int numFks) {
        if(numFks == 0) {
            return null;
        }
        final LRUMap<String, List<GridNode>>[] caches = new LRUMap[numFks];
        for(int i = 0; i < numFks; i++) {
            caches[i] = new LRUMap<String, List<GridNode>>(1000);
        }
        return caches;
    }

    @Nullable
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

    @Nullable
    private static int[] getParentTablesPartitionNo(final PrimaryKey childTablePkey, final DistributionCatalog catalog)
            throws GridException {
        final List<ForeignKey> parentFkeys = childTablePkey.getExportedKeys();
        if(parentFkeys == null) {
            return null;
        }
        final int numParents = parentFkeys.size();
        if(numParents == 0) {
            return null;
        }
        final int[] partitionNumbers = new int[numParents];
        for(int i = 0; i < numParents; i++) {
            ForeignKey parentFkey = parentFkeys.get(i);
            String table = parentFkey.getFkTableName();
            int tableId = catalog.getTableId(table, true);
            int partitionNo = DistributionCatalog.getTablePartitionNo(tableId);
            partitionNumbers[i] = partitionNo;
        }
        return partitionNumbers;
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

}

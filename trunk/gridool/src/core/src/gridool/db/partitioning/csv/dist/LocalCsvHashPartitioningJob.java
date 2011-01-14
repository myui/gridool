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
package gridool.db.partitioning.csv.dist;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.Settings;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.NodeWithPartitionNo;
import gridool.db.partitioning.csv.PartitioningJobConf;
import gridool.dfs.GridDfsClient;
import gridool.directory.ILocalDirectory;
import gridool.directory.btree.CallbackHandler;
import gridool.directory.btree.IndexException;
import gridool.directory.btree.Value;
import gridool.routing.GridRouter;
import gridool.util.collections.FixedArrayList;
import gridool.util.collections.LRUMap;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.ExecutorUtils;
import gridool.util.csv.CsvUtils;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.io.IOUtils;
import gridool.util.lang.ArrayUtils;
import gridool.util.primitive.MutableInt;
import gridool.util.primitive.Primitives;
import gridool.util.string.StringUtils;
import gridool.util.struct.Pair;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class LocalCsvHashPartitioningJob extends
        GridJobBase<Pair<PartitioningJobConf, GridRouter>, HashMap<GridNode, MutableInt>> {
    private static final long serialVersionUID = 149683992715077498L;
    private static final Log LOG = LogFactory.getLog(LocalCsvHashPartitioningJob.class);
    private static final int FK_INDEX_CACHE_SIZE = Primitives.parseInt(Settings.get("gridool.db.partitioning.fk_index_caches"), 8192);

    private transient HashMap<GridNode, MutableInt> assignedRecMap;

    @GridConfigResource
    private transient GridConfiguration config;
    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public LocalCsvHashPartitioningJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public boolean handleNodeFailure() {
        return false;
    }

    public Map<GridTask, GridNode> map(final GridRouter router, final Pair<PartitioningJobConf, GridRouter> args)
            throws GridException {
        assert (registry != null);
        final PartitioningJobConf ops = args.getFirst();
        final String[] lines = ops.getLines();
        final String csvFileName = ops.getFileName();
        final DBPartitioningJobConf jobConf = ops.getJobConf();
        final GridRouter origRouter = args.getSecond();

        // partitioning resources
        final String tableName;
        final int tablePartitionNo;
        final PrimaryKey primaryKey;
        final Collection<ForeignKey> foreignKeys;
        final int[] pkeyIndicies;
        final String[] parentTableFkIndexNames;
        final int numParentForeignKeys;
        final boolean hasParentTable;
        final int numForeignKeys;
        final String[] fkIdxNames;
        final int[][] fkPositions;
        final int[] childTablesPartitionNo;
        final LRUMap<String, List<NodeWithPartitionNo>>[] fkCaches;
        {
            tableName = jobConf.getTableName();
            DistributionCatalog catalog = registry.getDistributionCatalog();
            tablePartitionNo = catalog.getTablePartitionNo(tableName, true);
            Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys = ops.getPrimaryForeignKeys();
            primaryKey = primaryForeignKeys.getFirst();
            foreignKeys = primaryForeignKeys.getSecond();
            pkeyIndicies = primaryKey.getColumnPositions(true);
            parentTableFkIndexNames = GridDbUtils.getParentTableFkIndexNames(primaryKey);
            hasParentTable = (parentTableFkIndexNames != null);
            numParentForeignKeys = hasParentTable ? parentTableFkIndexNames.length : 0;
            numForeignKeys = foreignKeys.size();
            assert (numParentForeignKeys != 0);
            fkIdxNames = GridDbUtils.getFkIndexNames(foreignKeys, numForeignKeys);
            fkPositions = GridDbUtils.getFkPositions(foreignKeys, numForeignKeys);
            childTablesPartitionNo = GridDbUtils.getChildTablesPartitionNo(foreignKeys, numForeignKeys, catalog);
            fkCaches = GridDbUtils.getFkIndexCaches(numForeignKeys);
        }

        // COPY INTO control resources 
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        // working resources
        final ILocalDirectory index = registry.getDirectory();
        for(String idxName : fkIdxNames) {
            configureFkIndex(index, idxName, FK_INDEX_CACHE_SIZE);
        }
        final GridNode localNode = config.getLocalNode();
        final String[] fields = new String[GridDbUtils.getMaxColumnCount(primaryKey, foreignKeys)];
        assert (fields.length > 0);
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);
        final Charset charset = Charset.forName("UTF-8");
        final StringBuilder strBuf = new StringBuilder(64);
        final int totalRecords = lines.length;
        final String[] fkeysFields = new String[numForeignKeys];
        final byte[][] distkeys = new byte[numForeignKeys][];
        final GridNode[] fkMappedNodes = new GridNode[numForeignKeys];

        final int numNodes = origRouter.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new HashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Map<GridNode, MutableInt> mappedNodes = new HashMap<GridNode, MutableInt>(numNodes);
        final Map<GridNode, List<DerivedFragmentInfo>> idxShippingMap = new HashMap<GridNode, List<DerivedFragmentInfo>>(numNodes);
        for(int i = 0; i < totalRecords; i++) {
            String line = lines[i];
            lines[i] = null;
            // "primary" fragment mapping
            mapPrimaryFragment(mappedNodes, tablePartitionNo, localNode);
            if(hasParentTable) {
                // "derived by parent" fragment mapping
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                String pkeysField = GridDbUtils.combineFields(fields, pkeyIndicies.length, strBuf);
                final byte[] distkey = StringUtils.getBytes(pkeysField);
                for(int kk = 0; kk < numParentForeignKeys; kk++) {
                    String idxName = parentTableFkIndexNames[kk];
                    mapDerivedByParentFragment(distkey, mappedNodes, index, idxName);
                }
            }
            if(numForeignKeys > 0) {
                // "derived by child" fragment mapping
                for(int jj = 0; jj < numForeignKeys; jj++) {
                    int[] pos = fkPositions[jj];
                    CsvUtils.retrieveFields(line, pos, fieldList, filedSeparator, quoteChar);
                    fieldList.trimToZero();
                    String fkeysField = GridDbUtils.combineFields(fields, pos.length, strBuf);
                    byte[] distkey = StringUtils.getBytes(fkeysField);
                    fkMappedNodes[jj] = mapDerivedByChildFragment(distkey, mappedNodes, childTablesPartitionNo[jj], origRouter);
                    fkeysFields[jj] = fkeysField;
                    distkeys[jj] = distkey;
                }
                // store information for derived fragment mapping
                for(int kk = 0; kk < numForeignKeys; kk++) {
                    final String fkIdxName = fkIdxNames[kk];
                    final String fkeysField = fkeysFields[kk];
                    final byte[] distkey = distkeys[kk];
                    final GridNode fkMappedNode = fkMappedNodes[kk];
                    List<DerivedFragmentInfo> shipIdxList = idxShippingMap.get(fkMappedNode);
                    if(shipIdxList == null) {
                        shipIdxList = new ArrayList<DerivedFragmentInfo>(2000);
                        idxShippingMap.put(fkMappedNode, shipIdxList);
                    }
                    final LRUMap<String, List<NodeWithPartitionNo>> fkCache = fkCaches[kk];
                    List<NodeWithPartitionNo> storedNodeInfo = fkCache.get(fkeysField);
                    for(final Map.Entry<GridNode, MutableInt> e : mappedNodes.entrySet()) {
                        final GridNode node = e.getKey();
                        //if(node.equals(fkMappedNode)) continue; // no need to map
                        int hiddenValue = e.getValue().intValue();
                        NodeWithPartitionNo nodeInfo = new NodeWithPartitionNo(node, hiddenValue);
                        if(storedNodeInfo == null) {
                            storedNodeInfo = new ArrayList<NodeWithPartitionNo>(8);
                            fkCache.put(fkeysField, storedNodeInfo);
                        } else if(storedNodeInfo.contains(nodeInfo)) {// Note that node has unique hiddenValue to persist
                            continue;
                        }
                        storedNodeInfo.add(nodeInfo);
                        byte[] value = NodeWithPartitionNo.serialize(node, hiddenValue);
                        // index shipping 
                        DerivedFragmentInfo fragInfo = new DerivedFragmentInfo(fkIdxName, distkey, value);
                        shipIdxList.add(fragInfo);
                    }
                }
            }
            if(mappedNodes.isEmpty()) {
                throw new IllegalStateException("Could not map records for table: '" + tableName
                        + '\'');
            }
            // bind a record to nodes
            byte[] lineBytes = line.getBytes(charset);
            mapRecord(lineBytes, totalRecords, numNodes, nodeAssignMap, mappedNodes, filedSeparator);
            mappedNodes.clear();
        }

        final int numTasks = idxShippingMap.size();
        final Map<GridTask, GridNode> taskmap = new IdentityHashMap<GridTask, GridNode>(numTasks);
        for(final Map.Entry<GridNode, List<DerivedFragmentInfo>> e : idxShippingMap.entrySet()) {
            GridNode node = e.getKey();
            List<DerivedFragmentInfo> storeList = e.getValue();
            GridTask task = new GridBuildIndexTask(this, storeList);
            taskmap.put(task, node);
        }

        final ExecutorService sendExecs = ExecutorFactory.newFixedThreadPool(GridDfsClient.SENDER_CONCURRENCY, "FileSender", true);
        final GridDfsClient dfsClient = registry.getDfsService().getDFSClient();
        final int recvPort = config.getFileReceiverPort();
        final HashMap<GridNode, MutableInt> assignedRecMap = new HashMap<GridNode, MutableInt>(numNodes);
        for(final Map.Entry<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            GridNode trgNode = origRouter.resolve(node);
            Pair<MutableInt, FastByteArrayOutputStream> pair = e.getValue();
            MutableInt numRecords = pair.first;
            assignedRecMap.put(trgNode, numRecords);
            FastByteArrayOutputStream rows = pair.second;
            pair.clear();
            GridDbUtils.sendfile(sendExecs, dfsClient, csvFileName, rows, trgNode, recvPort);
        }

        for(final GridNode node : origRouter.getAllNodes()) {
            if(!assignedRecMap.containsKey(node)) {
                assignedRecMap.put(node, new MutableInt(0));
            }
        }
        this.assignedRecMap = assignedRecMap;

        ExecutorUtils.shutdownAndAwaitTermination(sendExecs);
        return taskmap;
    }

    private static void mapPrimaryFragment(final Map<GridNode, MutableInt> mappedNodes, final int tablePartitionNo, final GridNode localNode)
            throws GridException {
        MutableInt newHidden = new MutableInt(tablePartitionNo);
        mappedNodes.put(localNode, newHidden);
    }

    private static GridNode mapDerivedByChildFragment(final byte[] distkey, final Map<GridNode, MutableInt> mappedNodes, final int tablePartitionNo, final GridRouter router)
            throws GridException {
        GridNode mappedNode = router.selectNode(distkey);
        if(mappedNode == null) {
            throw new GridException("Could not find any node in cluster.");
        }
        final MutableInt hiddenValue = mappedNodes.get(mappedNode);
        if(hiddenValue == null) {
            mappedNodes.put(mappedNode, new MutableInt(tablePartitionNo));
        } else {
            int oldValue = hiddenValue.intValue();
            int newValue = oldValue | tablePartitionNo;
            hiddenValue.setValue(newValue);
        }
        return mappedNode;
    }

    private static void mapDerivedByParentFragment(final byte[] distkey, final Map<GridNode, MutableInt> mappedNodes, final ILocalDirectory index, final String parentTableFkIndex)
            throws GridException {
        final CallbackHandler handler = new CallbackHandler() {
            public boolean indexInfo(Value key, byte[] value) {
                int partitionNo = NodeWithPartitionNo.deserializePartitionNo(value);
                GridNode node = NodeWithPartitionNo.deserializeGridNode(value);

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
        } catch (IndexException e) {
            throw new GridException(e);
        }
    }

    private static void mapRecord(final byte[] line, final int totalRecords, final int numNodes, final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap, final Map<GridNode, MutableInt> mappedNodes, final char filedSeparator) {
        final int lineSize = line.length;
        for(final Map.Entry<GridNode, MutableInt> e : mappedNodes.entrySet()) {
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

    public HashMap<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

    private static void configureFkIndex(final ILocalDirectory index, final String idxName, final int cacheSize) {
        index.setCacheSize(idxName, cacheSize);
        index.setBulkloading(true, idxName);
    }

    static final class DerivedFragmentInfo
            implements Externalizable, Comparable<DerivedFragmentInfo> {
        private static final long serialVersionUID = -4472952971698389386L;

        private/* final */String fkIdxName;
        private/* final */byte[] distkey;
        private/* final */byte[] value;

        public DerivedFragmentInfo() {} // Externalizable

        DerivedFragmentInfo(@Nonnull String fkIdxName, @Nonnull byte[] distkey, byte[] value) {
            this.fkIdxName = fkIdxName;
            this.distkey = distkey;
            this.value = value;
        }

        String getFkIdxName() {
            return fkIdxName;
        }

        byte[] getDistkey() {
            return distkey;
        }

        byte[] getValue() {
            return value;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.fkIdxName = IOUtils.readString(in);
            this.distkey = IOUtils.readBytes(in);
            this.value = IOUtils.readBytes(in);
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(fkIdxName, out);
            IOUtils.writeBytes(distkey, out);
            IOUtils.writeBytes(value, out);
        }

        static DerivedFragmentInfo readFrom(ObjectInput in) throws IOException,
                ClassNotFoundException {
            DerivedFragmentInfo info = new DerivedFragmentInfo();
            info.readExternal(in);
            return info;
        }

        @Override
        public int compareTo(DerivedFragmentInfo other) {
            return ArrayUtils.compareTo(distkey, other.distkey);
        }

    }
}

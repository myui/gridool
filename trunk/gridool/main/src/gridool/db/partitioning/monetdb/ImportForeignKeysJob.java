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
package gridool.db.partitioning.monetdb;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridKernelResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
import gridool.db.sql.SQLTranslator;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.lang.ArrayUtils;
import xbird.util.struct.Pair;
import xbird.util.xfer.TransferUtils;

/**
 * 
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ImportForeignKeysJob extends GridJobBase<Pair<String, Boolean>, Boolean> {
    private static final long serialVersionUID = -1580857354649767246L;
    private static final Log LOG = LogFactory.getLog(ImportForeignKeysJob.class);

    private static final String WORK_DIR;
    static {
        WORK_DIR = GridUtils.getWorkDirPath();
    }

    @GridKernelResource
    private transient GridKernel kernel;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public ImportForeignKeysJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<String, Boolean> params)
            throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        String templateDbName = params.getFirst();
        boolean useGzip = params.getSecond();

        final ForeignKey[] fkeys = getForeignKeys(templateDbName);
        if(fkeys.length == 0) {
            throw new GridException("No foreign key found on template DB: " + templateDbName);
        }

        // #1 scatter missing foreign keys
        ScatterMissingReferencingKeysJobConf jobConf1 = new ScatterMissingReferencingKeysJobConf(fkeys, useGzip, nodes);
        GridJobFuture<DumpFile[]> future1 = kernel.execute(ScatterMissingReferencingKeysJob.class, jobConf1);
        DumpFile[] sentDumpedFiles = GridUtils.invokeGet(future1);

        // #2 retrieve missing referenced rows in exported tables
        RetrieveMissingReferencedRowsJobConf jobConf2 = new RetrieveMissingReferencedRowsJobConf(sentDumpedFiles, jobConf1);
        GridJobFuture<DumpFile[]> future2 = kernel.execute(RetrieveMissingReferenedRowsJob.class, jobConf2);
        DumpFile[] receivedDumpedFiles = GridUtils.invokeGet(future2);

        // #3 import collected missing foreign keys
        final int numNodes = nodes.length;
        final Map<GridNode, List<DumpFile>> dumpFileMapping = mapDumpFiles(receivedDumpedFiles, numNodes);
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        for(final GridNode node : nodes) {
            List<DumpFile> dumpFileList = dumpFileMapping.get(node);
            DumpFile[] dumpFiles = ArrayUtils.toArray(dumpFileList, DumpFile[].class);
            for(DumpFile df : dumpFiles) {
                df.clearAssociatedNode();
            }
            GridTask task = new ImportCollectedExportedKeysTask(this, fkeys, dumpFiles);
            map.put(task, node);
        }
        return map;
    }

    @Nonnull
    private ForeignKey[] getForeignKeys(final String templateDbName) throws GridException {
        final DBAccessor dba = registry.getDbAccessor();
        final Connection conn;
        try {
            conn = dba.getConnection(templateDbName);
        } catch (SQLException e) {
            throw new GridException(e);
        }
        final Collection<ForeignKey> fkeys;
        try {
            fkeys = GridDbUtils.getForeignKeys(conn);
        } catch (SQLException e) {
            throw new GridException(e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
        return ArrayUtils.toArray(fkeys, ForeignKey[].class);
    }

    private static Map<GridNode, List<DumpFile>> mapDumpFiles(final DumpFile[] dumpFiles, final int numNodes) {
        final Map<GridNode, List<DumpFile>> map = new HashMap<GridNode, List<DumpFile>>(numNodes);
        for(final DumpFile file : dumpFiles) {
            GridNode node = file.getAssociatedNode();
            List<DumpFile> fileList = map.get(node);
            if(fileList == null) {
                fileList = new ArrayList<DumpFile>(numNodes);
                map.put(node, fileList);
            }
            fileList.add(file);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Boolean res = result.getResult();
        if(Boolean.TRUE != res) {
            GridException error = result.getException();
            GridNode executedNode = result.getExecutedNode();
            throw new GridException("ImportCollectedExportedKeysTask failed on node: "
                    + executedNode, error);
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return Boolean.TRUE;
    }

    public static final class ScatterMissingReferencingKeysJob extends
            GridJobBase<ScatterMissingReferencingKeysJobConf, DumpFile[]> {
        private static final long serialVersionUID = -7341912223637268324L;

        private transient List<DumpFile> dumpedFileList;

        public ScatterMissingReferencingKeysJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, ScatterMissingReferencingKeysJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final int numNodes = nodes.length;
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
            for(final GridNode node : nodes) {
                GridTask task = new ScatterMissingReferencingKeysTask(this, jobConf);
                map.put(task, node);
            }
            this.dumpedFileList = new ArrayList<DumpFile>(numNodes * 10);
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            final DumpFile[] dumpedFiles = result.getResult();
            if(dumpedFiles == null) {
                GridNode executedNode = result.getExecutedNode();
                throw new GridException("CreateMissingImportedKeyViewTask has no return found on node: "
                        + executedNode);
            }
            for(final DumpFile f : dumpedFiles) {
                dumpedFileList.add(f);
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public DumpFile[] reduce() throws GridException {
            return ArrayUtils.toArray(dumpedFileList, DumpFile[].class);
        }

    }

    private static final class ScatterMissingReferencingKeysTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1012236314682018854L;

        private final ForeignKey[] fkeys;
        private final boolean useGzip;
        private final GridNode[] dstNodes;

        @GridConfigResource
        private transient GridConfiguration config;
        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        ScatterMissingReferencingKeysTask(@Nonnull GridJob job, @Nonnull ScatterMissingReferencingKeysJobConf jobConf) {
            super(job, false);
            this.fkeys = jobConf.getForeignKeys();
            this.useGzip = jobConf.isUseGzip();
            this.dstNodes = jobConf.getNodes();
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        /**
         * @return <filePath[], updatedRow[]>
         */
        @Override
        protected DumpFile[] execute() throws GridException {
            final Map<String, List<ForeignKey>> tableRefMap = getTableReferencesMap(fkeys);
            final GridNode localNode = config.getLocalNode();

            final List<DumpFile> dumpList;
            DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true);
            try {
                // #1 dump missing foreign keys into files
                dumpList = dumpMissingForeignKeysIntoFiles(conn, tableRefMap, localNode, useGzip);
            } catch (SQLException e) {
                LOG.error(e);
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }

            // #2 scatter dumped file
            final int port = config.getFileReceiverPort();
            try {
                scatterDumpedViewFiles(dumpList, port, dstNodes, localNode);
            } catch (IOException e) {
                throw new GridException(e);
            } finally {
                for(DumpFile dumpFile : dumpList) {
                    String filePath = dumpFile.getFilePath();
                    File file = new File(filePath);
                    if(!file.delete()) {
                        LOG.warn("Cannot delete a file: " + file.getAbsolutePath());
                    }
                }
            }
            return ArrayUtils.toArray(dumpList, DumpFile[].class);
        }

        private static Map<String, List<ForeignKey>> getTableReferencesMap(final ForeignKey[] fkeys) {
            final Map<String, List<ForeignKey>> map = new HashMap<String, List<ForeignKey>>(16);
            for(final ForeignKey fk : fkeys) {
                final String pkTable = fk.getPkTableName();
                List<ForeignKey> fkList = map.get(pkTable);
                if(fkList == null) {
                    fkList = new ArrayList<ForeignKey>(4);
                    map.put(pkTable, fkList);
                } else {
                    List<String> pkColumns = fk.getPkColumnNames();
                    ForeignKey otherFk = fkList.get(0);
                    List<String> otherPkColumns = otherFk.getPkColumnNames();
                    if(!pkColumns.equals(otherPkColumns)) {
                        throw new UnsupportedOperationException("Unsupported condition: Referenced columns of a table '"
                                + pkTable + "' differs");
                    }
                }
                fkList.add(fk);
            }
            return map;
        }

        private static List<DumpFile> dumpMissingForeignKeysIntoFiles(final Connection conn, final Map<String, List<ForeignKey>> tableRefMap, final GridNode localNode, final boolean gzip)
                throws SQLException, GridException {
            final String localNodeId = getIdentitifier(localNode);
            final int numTables = tableRefMap.size();
            final List<DumpFile> dumpedFiles = new ArrayList<DumpFile>(numTables);
            for(Map.Entry<String, List<ForeignKey>> e : tableRefMap.entrySet()) {
                String pkTableName = e.getKey();
                String fileName = pkTableName + ".dump." + localNodeId + (gzip ? ".gz" : "");
                String filePath = WORK_DIR + File.separatorChar + fileName;
                File file = new File(filePath);
                if(file.exists()) {
                    LOG.warn("File already exists: " + file.getAbsolutePath());
                } else {
                    final boolean created;
                    try {
                        created = file.createNewFile();
                    } catch (IOException ioe) {
                        String errmsg = "Cannot create a file: " + file.getAbsolutePath();
                        LOG.error(errmsg, ioe);
                        throw new GridException(errmsg, ioe);
                    }
                    if(!created) {
                        String errmsg = "Cannot create a file: " + file.getAbsolutePath();
                        LOG.error(errmsg);
                        throw new GridException(errmsg);
                    }
                }
                List<ForeignKey> fkeys = e.getValue();
                String subquery = makeSelectMissingForeignKeyQuery(fkeys);
                String query = GridDbUtils.makeCopyIntoFileQuery(subquery, filePath);
                if(LOG.isInfoEnabled()) {
                    LOG.info("Dump missing referenced keys in exported table '" + pkTableName
                            + ":\n" + query);
                }
                int updatedRows = JDBCUtils.update(conn, query);
                if(updatedRows > 0) {
                    List<String> pkColumnNames = fkeys.get(0).getPkColumnNames();
                    DumpFile dumpFile = new DumpFile(file, pkTableName, pkColumnNames, updatedRows, localNode);
                    dumpedFiles.add(dumpFile);
                }
            }
            return dumpedFiles;
        }

        private static String makeSelectMissingForeignKeyQuery(final List<ForeignKey> fkeys) {
            final int numFkeys = fkeys.size();
            if(numFkeys == 0) {
                throw new IllegalArgumentException("No foreign key");
            }
            final StringBuilder buf = new StringBuilder(512);
            for(int i = 0; i < numFkeys; i++) {
                final ForeignKey fk = fkeys.get(i);
                if(i != 0) {
                    buf.append("\nUNION\n");
                }
                buf.append("SELECT ");
                if(numFkeys == 1) {
                    buf.append("DISTINCT ");
                }
                final List<String> fkColumns = fk.getFkColumnNames();
                final int numFkColumns = fkColumns.size();
                for(int j = 0; j < numFkColumns; j++) {
                    if(j != 0) {
                        buf.append(',');
                    }
                    buf.append("l.");
                    String fkColumn = fkColumns.get(j);
                    buf.append(fkColumn);
                }
                buf.append("\nFROM \"");
                buf.append(fk.getFkTableName());
                buf.append("\" l LEFT OUTER JOIN \"");
                buf.append(fk.getPkTableName());
                buf.append("\" r ON ");
                final List<String> pkColumns = fk.getPkColumnNames();
                final int numPkColumns = pkColumns.size();
                if(numFkColumns != numPkColumns) {
                    throw new IllegalStateException("numFkColumns(" + numFkColumns
                            + ") != numPkColumns(" + numPkColumns + ')');
                }
                for(int j = 0; j < numPkColumns; j++) {
                    if(j != 0) {
                        buf.append(" AND ");
                    }
                    buf.append("l.\"");
                    String fkc = fkColumns.get(j);
                    buf.append(fkc);
                    buf.append("\" = r.\"");
                    String pkc = pkColumns.get(j);
                    buf.append(pkc);
                    buf.append('"');
                }
                buf.append("\nWHERE ");
                for(int j = 0; j < numFkColumns; j++) {
                    if(j != 0) {
                        buf.append(" AND ");
                    }
                    buf.append("r.\"");
                    String fkc = pkColumns.get(j);
                    buf.append(fkc);
                    buf.append("\" IS NULL");
                }
            }
            return buf.toString();
        }

        private static void scatterDumpedViewFiles(final List<DumpFile> dumpList, final int dstPort, final GridNode[] dstNodes, final GridNode localNode)
                throws IOException {
            for(final DumpFile dumpFile : dumpList) {
                for(final GridNode node : dstNodes) {
                    if(!node.equals(localNode)) {
                        String filePath = dumpFile.getFilePath();
                        File file = new File(filePath);
                        if(!file.exists()) {
                            throw new IllegalStateException("Dumped file does not exist: "
                                    + file.getAbsolutePath());
                        }
                        InetAddress addr = node.getPhysicalAdress();
                        TransferUtils.sendfile(file, addr, dstPort, false, true);
                    }
                }
            }
        }

    }

    static final class ScatterMissingReferencingKeysJobConf implements Externalizable {

        @Nonnull
        private/* final */ForeignKey[] fkeys;
        private/* final */boolean useGzip;
        @Nonnull
        private/* final */GridNode[] nodes;

        public ScatterMissingReferencingKeysJobConf() {} // Externalizable

        ScatterMissingReferencingKeysJobConf(@CheckForNull ForeignKey[] fkeys, boolean useGzip, @CheckForNull GridNode[] nodes) {
            if(fkeys == null || fkeys.length == 0) {
                throw new IllegalArgumentException("ForeignKeys are required: "
                        + Arrays.toString(fkeys));
            }
            if(nodes == null || nodes.length == 0) {
                throw new IllegalArgumentException("Nodes are required: " + Arrays.toString(nodes));
            }
            this.fkeys = fkeys;
            this.useGzip = useGzip;
            this.nodes = nodes;
        }

        @Nonnull
        public ForeignKey[] getForeignKeys() {
            return fkeys;
        }

        public boolean isUseGzip() {
            return useGzip;
        }

        @Nonnull
        public GridNode[] getNodes() {
            return nodes;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            final int numFkeys = in.readInt();
            final ForeignKey[] l_fkeys = new ForeignKey[numFkeys];
            for(int i = 0; i < numFkeys; i++) {
                l_fkeys[i] = (ForeignKey) in.readObject();
            }
            this.fkeys = l_fkeys;
            this.useGzip = in.readBoolean();
            final int numNodes = in.readInt();
            final GridNode[] l_nodes = new GridNode[numNodes];
            for(int i = 0; i < numNodes; i++) {
                l_nodes[i] = (GridNode) in.readObject();
            }
            this.nodes = l_nodes;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(fkeys.length);
            for(int i = 0; i < fkeys.length; i++) {
                out.writeObject(fkeys[i]);
            }
            out.writeBoolean(useGzip);
            out.writeInt(nodes.length);
            for(int i = 0; i < nodes.length; i++) {
                out.writeObject(nodes[i]);
            }
        }

    }

    static final class DumpFile implements Externalizable {

        private/* final */String fileName;
        private/* final */String tableName;
        private/* final */List<String> columnNames;
        private/* final */int records;
        private/* final */GridNode assocNode;

        @Nullable
        private transient File file;
        @Nullable
        private transient String filePath;

        public DumpFile() {}//Externalizable

        public DumpFile(@Nonnull File file, @Nonnull String tableName, @Nonnull List<String> columnNames, @Nonnegative int records, @Nonnull GridNode assocNode) {
            if(records < 1) {
                throw new IllegalArgumentException("Illegal records: " + records);
            }
            this.fileName = file.getName();
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.records = records;
            this.assocNode = assocNode;
            this.file = file;
            this.filePath = file.getAbsolutePath();
        }

        public DumpFile(@Nonnull File file, @Nonnull String tableName, @Nonnegative int records, @Nonnull GridNode assocNode) {
            if(records < 1) {
                throw new IllegalArgumentException("Illegal records: " + records);
            }
            this.fileName = file.getName();
            this.tableName = tableName;
            this.columnNames = Collections.emptyList();
            this.records = records;
            this.assocNode = assocNode;
            this.file = file;
            this.filePath = file.getAbsolutePath();
        }

        @Nonnull
        String getFileName() {
            return fileName;
        }

        @Nonnull
        String getTableName() {
            return tableName;
        }

        @Nonnull
        List<String> getColumnNames() {
            return columnNames;
        }

        @Nonnegative
        int getRecords() {
            return records;
        }

        @Nonnull
        GridNode getAssociatedNode() {
            if(assocNode == null) {
                throw new IllegalStateException("DumpFile#getAssociatedNode() should not be called when associated node is null");
            }
            return assocNode;
        }

        void clearAssociatedNode() {
            this.assocNode = null;
        }

        @Nonnull
        String getFilePath() {
            if(filePath == null) {
                this.filePath = getFile().getAbsolutePath();
            }
            return filePath;
        }

        @Nonnull
        File getFile() {
            if(file == null) {
                File colDir = GridUtils.getWorkDir();
                this.file = new File(colDir, fileName);
            }
            return file;
        }

        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            this.fileName = IOUtils.readString(in);
            this.tableName = IOUtils.readString(in);
            final int numColumns = in.readInt();
            if(numColumns == 0) {
                this.columnNames = Collections.emptyList();
            } else {
                this.columnNames = new ArrayList<String>(numColumns);
                for(int i = 0; i < numColumns; i++) {
                    String name = IOUtils.readString(in);
                    columnNames.add(name);
                }
            }
            this.records = in.readInt();
            boolean hasAssocNode = in.readBoolean();
            if(hasAssocNode) {
                this.assocNode = (GridNode) in.readObject();
            }
        }

        public void writeExternal(final ObjectOutput out) throws IOException {
            IOUtils.writeString(fileName, out);
            IOUtils.writeString(tableName, out);
            final int numColumns = columnNames.size();
            out.writeInt(numColumns);
            if(numColumns > 0) {
                for(String name : columnNames) {
                    IOUtils.writeString(name, out);
                }
            }
            out.writeInt(records);
            if(assocNode == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeObject(assocNode);
            }
        }

    }

    public static final class RetrieveMissingReferenedRowsJob extends
            GridJobBase<RetrieveMissingReferencedRowsJobConf, DumpFile[]> {
        private static final long serialVersionUID = -1419333559953426203L;

        private transient List<DumpFile> dumpedFileList;

        public RetrieveMissingReferenedRowsJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, RetrieveMissingReferencedRowsJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                GridTask task = new RetrieveMissingReferencedRowsTask(this, jobConf);
                map.put(task, node);
            }
            this.dumpedFileList = new ArrayList<DumpFile>(jobConf.getDumpedFiles().length);
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            final DumpFile[] dumpedFiles = result.getResult();
            if(dumpedFiles == null) {
                GridNode executedNode = result.getExecutedNode();
                throw new GridException("RetrieveMissingForeignKeysTask has no return found on node: "
                        + executedNode);
            }
            for(final DumpFile f : dumpedFiles) {
                dumpedFileList.add(f);
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public DumpFile[] reduce() throws GridException {
            return ArrayUtils.toArray(dumpedFileList, DumpFile[].class);
        }

    }

    private static final class RetrieveMissingReferencedRowsTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1263155385842261227L;

        private final RetrieveMissingReferencedRowsJobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;
        @GridConfigResource
        private transient GridConfiguration config;

        @SuppressWarnings("unchecked")
        RetrieveMissingReferencedRowsTask(GridJob job, RetrieveMissingReferencedRowsJobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected DumpFile[] execute() throws GridException {
            final DBAccessor dba = registry.getDbAccessor();
            final GridNode localNode = config.getLocalNode();
            final String localNodeId = getIdentitifier(localNode);
            final int dstPort = config.getFileReceiverPort();

            final DumpFile[] dumpedInputFiles = jobConf.getDumpedFiles();
            final List<DumpFile> dumpedOutputFiles = new ArrayList<DumpFile>(dumpedInputFiles.length);
            for(final DumpFile dumpFile : dumpedInputFiles) {
                final GridNode origNode = dumpFile.getAssociatedNode();
                if(!origNode.equals(localNode)) {
                    final DumpFile output;
                    try {
                        output = performQuery(dba, dumpFile, localNodeId);
                    } finally {
                        File inputFile = dumpFile.getFile();
                        if(!inputFile.delete()) {
                            LOG.warn("Failed to delete a input dump file: "
                                    + inputFile.getAbsolutePath());
                        }
                    }
                    if(output == null) {
                        continue;
                    }
                    InetAddress dstAddr = origNode.getPhysicalAdress();
                    try {
                        TransferUtils.sendfile(output.getFile(), dstAddr, dstPort, false, true);
                    } catch (IOException e) {
                        throw new GridException(e);
                    } finally {
                        File file = output.getFile();
                        if(!file.delete()) {
                            LOG.warn("Failed to delete a file: " + file.getAbsolutePath());
                        }
                    }
                    dumpedOutputFiles.add(output);
                }
            }
            return ArrayUtils.toArray(dumpedOutputFiles, DumpFile[].class);
        }

        @Nullable
        private DumpFile performQuery(final DBAccessor dba, final DumpFile dumpFile, final String localNodeId)
                throws GridException {
            final File outputFile = prepareResponseFile(dumpFile, localNodeId, jobConf.isUseGzip());

            final int affectedRows;
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
            try {
                String importedTableName = prepareTempolaryTable(conn, dumpFile);
                affectedRows = dumpRequiredRows(conn, dumpFile, importedTableName, outputFile.getAbsolutePath(), registry.getDistributionCatalog());
            } catch (SQLException e) {
                LOG.error(e);
                if(!outputFile.delete()) {
                    LOG.warn("Failed to delete a file: " + outputFile.getAbsolutePath());
                }
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            if(affectedRows > 0) {
                String dumpedTableName = dumpFile.getTableName();
                GridNode origNode = dumpFile.getAssociatedNode();
                return new DumpFile(outputFile, dumpedTableName, affectedRows, origNode);
            } else {
                return null;
            }
        }

        private static File prepareResponseFile(final DumpFile dumpFile, final String localNodeId, final boolean gzip)
                throws GridException {
            String tableName = dumpFile.getTableName();
            GridNode origNode = dumpFile.getAssociatedNode();
            String toNodeId = getIdentitifier(origNode);
            String outFilePath = WORK_DIR + File.separatorChar + tableName + ".dump.f"
                    + localNodeId + 't' + toNodeId + (gzip ? ".gz" : "");
            final File outFile = new File(outFilePath);
            if(outFile.exists()) {
                LOG.warn("Output file already exists: " + outFile.getAbsolutePath());
            } else {
                try {
                    if(!outFile.createNewFile()) {
                        throw new GridException("Cannot create a file: "
                                + outFile.getAbsolutePath());
                    }
                } catch (IOException e) {
                    throw new GridException("Failed to create a file: " + outFile.getAbsolutePath(), e);
                }
            }
            return outFile;
        }

        private static String prepareTempolaryTable(final Connection conn, final DumpFile dumpFile)
                throws SQLException, GridException {
            // create a temp table to load a incoming dump file
            final StringBuilder subquery = new StringBuilder(128);
            subquery.append("SELECT ");
            final List<String> columns = dumpFile.getColumnNames();
            final int numColumns = columns.size();
            for(int i = 0; i < numColumns; i++) {
                if(i != 0) {
                    subquery.append(',');
                }
                String name = columns.get(i);
                subquery.append('"');
                subquery.append(name);
                subquery.append('"');
            }
            subquery.append(" FROM \"");
            String tableName = dumpFile.getTableName();
            subquery.append(tableName);
            subquery.append("\" WHERE false");

            GridNode node = dumpFile.getAssociatedNode();
            String tmpTableName = tableName + '_' + getIdentitifier(node);
            final String createTable = "CREATE LOCAL TEMPORARY TABLE \"" + tmpTableName + "\" AS ("
                    + subquery.toString() + ") WITH NO DATA";
            JDBCUtils.update(conn, createTable);

            // invoke copy into table
            final int expectedRecords = dumpFile.getRecords();
            final String copyIntoTable = GridDbUtils.makeCopyIntoTableQuery(tmpTableName, dumpFile.getFilePath(), expectedRecords);
            final int updatedRecords = JDBCUtils.update(conn, copyIntoTable);
            if(expectedRecords != updatedRecords) {
                throw new GridException("Expected records to import (" + expectedRecords
                        + ") != Actual records imported (" + updatedRecords + "):\n"
                        + copyIntoTable);
            }
            return tmpTableName;
        }

        private static int dumpRequiredRows(final Connection conn, final DumpFile dumpFile, final String importedTableName, final String outputFilePath, final DistributionCatalog catalog)
                throws GridException, SQLException {
            final StringBuilder queryBuf = new StringBuilder(256);
            queryBuf.append("SELECT l.* FROM \"");
            final String lhsTableName = dumpFile.getTableName();
            queryBuf.append(lhsTableName);
            queryBuf.append("\" l, \"");
            queryBuf.append(importedTableName);
            queryBuf.append("\" r WHERE ");
            queryBuf.append(lhsTableName);
            queryBuf.append(" partitioned by (");
            final List<String> columns = dumpFile.getColumnNames();
            final int numColumns = columns.size();
            for(int i = 0; i < numColumns; i++) {
                if(i != 0) {
                    queryBuf.append(',');
                }
                String column = columns.get(i);
                queryBuf.append(column);
            }
            queryBuf.append(")");
            queryBuf.append(" alias l");
            for(int i = 0; i < numColumns; i++) {
                queryBuf.append(" AND l.\"");
                String column = columns.get(i);
                queryBuf.append(column);
                queryBuf.append("\" = r.\"");
                queryBuf.append(column);
                queryBuf.append('"');
            }
            SQLTranslator trans = new SQLTranslator(catalog);
            String subquery = trans.translateQuery(queryBuf.toString());
            String copyIntoFile = GridDbUtils.makeCopyIntoFileQuery(subquery, outputFilePath);
            return JDBCUtils.update(conn, copyIntoFile);
        }

    }

    static final class RetrieveMissingReferencedRowsJobConf implements Externalizable {

        private/* final */DumpFile[] dumpedFiles;
        private/* final */GridNode[] nodes;
        private/* final */boolean useGzip;

        public RetrieveMissingReferencedRowsJobConf() {}// Externalizable

        RetrieveMissingReferencedRowsJobConf(@CheckForNull DumpFile[] dumpedFiles, ScatterMissingReferencingKeysJobConf jobConf) {
            if(dumpedFiles == null) {
                throw new IllegalArgumentException();
            }
            this.dumpedFiles = dumpedFiles;
            this.nodes = jobConf.getNodes();
            this.useGzip = jobConf.isUseGzip();
        }

        @Nonnull
        public DumpFile[] getDumpedFiles() {
            return dumpedFiles;
        }

        @Nonnull
        public GridNode[] getNodes() {
            return nodes;
        }

        public boolean isUseGzip() {
            return useGzip;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            final int numDumpedFiles = in.readInt();
            final DumpFile[] df = new DumpFile[numDumpedFiles];
            for(int i = 0; i < numDumpedFiles; i++) {
                df[i] = (DumpFile) in.readObject();
            }
            this.dumpedFiles = df;
            final int numNodes = in.readInt();
            final GridNode[] nodes = new GridNode[numNodes];
            for(int i = 0; i < numNodes; i++) {
                nodes[i] = (GridNode) in.readObject();
            }
            this.nodes = nodes;
            this.useGzip = in.readBoolean();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(dumpedFiles.length);
            for(final DumpFile df : dumpedFiles) {
                out.writeObject(df);
            }
            out.writeInt(nodes.length);
            for(final GridNode node : nodes) {
                out.writeObject(node);
            }
            out.writeBoolean(useGzip);
        }

    }

    private static final class ImportCollectedExportedKeysTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1192299061445569050L;

        @Nonnull
        private final ForeignKey[] fkeys;
        @Nonnull
        private final DumpFile[] receivedDumpedFiles;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        ImportCollectedExportedKeysTask(@Nonnull GridJob job, @CheckForNull ForeignKey[] fkeys, @CheckForNull DumpFile[] receivedDumpedFiles) {
            super(job, false);
            if(fkeys == null) {
                throw new IllegalArgumentException();
            }
            if(fkeys.length == 0) {
                throw new IllegalArgumentException();
            }
            if(receivedDumpedFiles == null) {
                throw new IllegalArgumentException();
            }
            this.fkeys = fkeys;
            this.receivedDumpedFiles = receivedDumpedFiles;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Boolean execute() throws GridException {
            DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
            try {
                loadRequiredRecords(conn, receivedDumpedFiles);
                addForeignKeyConstraints(conn, fkeys);
                conn.commit();
            } catch (SQLException e) {
                LOG.error(e);
                try {
                    conn.rollback();
                } catch (SQLException rbe) {
                    LOG.warn("Rollback failed", rbe);
                }
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
                for(DumpFile dumpFile : receivedDumpedFiles) {
                    File file = dumpFile.getFile();
                    if(file.exists()) {
                        if(!file.delete()) {
                            LOG.warn("Failed to delete a file: " + file.getAbsolutePath());
                        }
                    }
                }
            }
            return Boolean.TRUE;
        }

        private static void loadRequiredRecords(final Connection conn, final DumpFile[] dumpedFiles)
                throws SQLException, GridException {
            for(final DumpFile dumpFile : dumpedFiles) {
                String tableName = dumpFile.getTableName();
                String inputFilePath = dumpFile.getFilePath();
                final int expectedRecords = dumpFile.getRecords();
                String copyIntoTable = GridDbUtils.makeCopyIntoTableQuery(tableName, inputFilePath, expectedRecords);
                final int updatedRecords = JDBCUtils.update(conn, copyIntoTable);
                if(expectedRecords != updatedRecords) {
                    throw new GridException("Expected records to import (" + expectedRecords
                            + ") != Actual records imported (" + updatedRecords + "):\n"
                            + copyIntoTable);
                }
                File UnNeededfile = dumpFile.getFile();
                if(!UnNeededfile.delete()) {
                    LOG.warn("Failed to delete a file: " + UnNeededfile.getAbsolutePath());
                }
            }
        }

        private static void addForeignKeyConstraints(final Connection conn, final ForeignKey[] fkeys)
                throws SQLException {
            final StringBuilder queryBuf = new StringBuilder(256);
            for(final ForeignKey fk : fkeys) {
                queryBuf.append("ALTER TABLE \"");
                queryBuf.append(fk.getFkTableName());
                queryBuf.append("\" ADD CONSTRAINT \"");
                queryBuf.append(fk.getFkName());
                queryBuf.append("\" FOREIGN KEY (");
                final List<String> fkColumns = fk.getFkColumnNames();
                final int numFkColumns = fkColumns.size();
                for(int i = 0; i < numFkColumns; i++) {
                    if(i != 0) {
                        queryBuf.append(',');
                    }
                    queryBuf.append('"');
                    String column = fkColumns.get(i);
                    queryBuf.append(column);
                    queryBuf.append('"');
                }
                queryBuf.append(") REFERENCES ");
                queryBuf.append(fk.getPkTableName());
                queryBuf.append(" (");
                final List<String> pkColumns = fk.getPkColumnNames();
                final int numPkColumns = pkColumns.size();
                for(int i = 0; i < numPkColumns; i++) {
                    if(i != 0) {
                        queryBuf.append(',');
                    }
                    queryBuf.append('"');
                    String column = pkColumns.get(i);
                    queryBuf.append(column);
                    queryBuf.append('"');
                }
                queryBuf.append(");\n");
            }
            String sql = queryBuf.toString();
            if(LOG.isInfoEnabled()) {
                LOG.info("Add foreign key constraints: \n" + sql);
            }
            JDBCUtils.update(conn, sql);
        }

    }

    private static String getIdentitifier(final GridNode node) {
        return node.getPhysicalAdress().getHostAddress().replace(".", "") + node.getPort();
    }

}
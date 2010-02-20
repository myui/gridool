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
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        // #1 create view for missing foreign keys
        CreateMissingImportedKeyViewJobConf jobConf1 = makeJobConf(templateDbName, useGzip, nodes);
        GridJobFuture<DumpFile[]> future1 = kernel.execute(CreateMissingImportedKeyViewJob.class, jobConf1);
        DumpFile[] sentDumpedFiles = GridUtils.invokeGet(future1);

        // #2 ship missing foreign keys and retrieve referencing data
        RetrieveMissingForeignKeysJobConf jobConf2 = new RetrieveMissingForeignKeysJobConf(sentDumpedFiles, jobConf1);
        GridJobFuture<DumpFile[]> future2 = kernel.execute(RetrieveMissingForeignKeysJob.class, jobConf2);
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
            GridTask task = new ImportCollectedExportedKeysTask(this, dumpFiles);
            map.put(task, node);
        }
        return map;
    }

    private CreateMissingImportedKeyViewJobConf makeJobConf(final String templateDbName, final boolean useGzip, final GridNode[] nodes)
            throws GridException {
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
        String viewNamePrefix = Integer.toHexString(System.identityHashCode(this))
                + System.nanoTime();
        ForeignKey[] fkeyArray = ArrayUtils.toArray(fkeys, ForeignKey[].class);
        return new CreateMissingImportedKeyViewJobConf(viewNamePrefix, fkeyArray, useGzip, nodes);
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

    public static final class CreateMissingImportedKeyViewJob extends
            GridJobBase<CreateMissingImportedKeyViewJobConf, DumpFile[]> {
        private static final long serialVersionUID = -7341912223637268324L;

        private transient List<DumpFile> dumpedFileList;

        public CreateMissingImportedKeyViewJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, CreateMissingImportedKeyViewJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final int numNodes = nodes.length;
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
            for(final GridNode node : nodes) {
                GridTask task = new CreateMissingImportedKeyViewTask(this, jobConf);
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

    private static final class CreateMissingImportedKeyViewTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1012236314682018854L;

        private final ForeignKey[] fkeys;
        private final String viewNamePrefix;
        private final boolean useGzip;
        private final GridNode[] dstNodes;

        @GridConfigResource
        private transient GridConfiguration config;
        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        CreateMissingImportedKeyViewTask(@Nonnull GridJob job, @Nonnull CreateMissingImportedKeyViewJobConf jobConf) {
            super(job, false);
            this.fkeys = jobConf.getForeignKeys();
            this.viewNamePrefix = jobConf.getViewNamePrefix();
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
            final GridNode localNode = config.getLocalNode();
            final String query = getCreateViewQuery(fkeys, viewNamePrefix);

            final List<DumpFile> dumpList;
            DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
            try {
                // #1 create view for missing foreign keys
                JDBCUtils.update(conn, query);
                // #2 dump missing foreign keys into files
                dumpList = dumpViewsIntoFiles(conn, fkeys, viewNamePrefix, localNode, useGzip);
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
            }

            // #3 scatter dumped file
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

        private static String getCreateViewQuery(final ForeignKey[] fkeys, final String viewNamePrefix) {
            final StringBuilder buf = new StringBuilder(512);
            for(final ForeignKey fk : fkeys) {
                buf.append("CREATE VIEW \"");
                String viewName = viewNamePrefix + fk.getFkName();
                buf.append(viewName);
                buf.append("\" AS (\nSELECT DISTINCT ");
                final List<String> fkColumns = fk.getFkColumnNames();
                final int numFkColumns = fkColumns.size();
                for(int i = 0; i < numFkColumns; i++) {
                    if(i != 0) {
                        buf.append(',');
                    }
                    buf.append("l.");
                    String fkColumn = fkColumns.get(i);
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
                for(int i = 0; i < numPkColumns; i++) {
                    if(i != 0) {
                        buf.append(" AND ");
                    }
                    buf.append("l.\"");
                    String fkc = fkColumns.get(i);
                    buf.append(fkc);
                    buf.append("\" = r.\"");
                    String pkc = pkColumns.get(i);
                    buf.append(pkc);
                    buf.append('"');
                }
                buf.append("\nWHERE ");
                for(int i = 0; i < numFkColumns; i++) {
                    if(i != 0) {
                        buf.append(" AND ");
                    }
                    buf.append("r.\"");
                    String fkc = pkColumns.get(i);
                    buf.append(fkc);
                    buf.append("\" IS NULL");
                }
                buf.append("\n);\n");
            }
            return buf.toString();
        }

        private static List<DumpFile> dumpViewsIntoFiles(final Connection conn, final ForeignKey[] fkeys, final String viewNamePrefix, final GridNode localNode, final boolean gzip)
                throws SQLException, GridException {
            final String localNodeId = getIdentitifier(localNode);
            final int numFkeys = fkeys.length;
            final List<DumpFile> dumpedFiles = new ArrayList<DumpFile>(numFkeys);
            for(int i = 0; i < numFkeys; i++) {
                ForeignKey fk = fkeys[i];
                String fkName = fk.getFkName();
                String viewName = viewNamePrefix + fkName;
                String fileName = fkName + ".dump." + (gzip ? (localNodeId + ".gz") : localNodeId);
                String filePath = WORK_DIR + File.separatorChar + fileName;
                File file = new File(filePath);
                if(file.exists()) {
                    LOG.warn("File already exists: " + file.getAbsolutePath());
                } else {
                    final boolean created;
                    try {
                        created = file.createNewFile();
                    } catch (IOException e) {
                        String errmsg = "Cannot create a file: " + file.getAbsolutePath();
                        LOG.error(errmsg, e);
                        throw new GridException(errmsg, e);
                    }
                    if(!created) {
                        String errmsg = "Cannot create a file: " + file.getAbsolutePath();
                        LOG.error(errmsg);
                        throw new GridException(errmsg);
                    }
                }
                String subquery = "SELECT * FROM \"" + viewName + '"';
                String query = GridDbUtils.makeCopyIntoFileQuery(subquery, filePath);
                int updatedRows = JDBCUtils.update(conn, query);
                if(updatedRows > 0) {
                    DumpFile dumpFile = new DumpFile(fileName, filePath, updatedRows, fk, localNode);
                    dumpedFiles.add(dumpFile);
                }
            }
            return dumpedFiles;
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

    static final class CreateMissingImportedKeyViewJobConf implements Externalizable {

        @Nonnull
        private/* final */String viewNamePrefix;
        @Nonnull
        private/* final */ForeignKey[] fkeys;
        private/* final */boolean useGzip;
        @Nonnull
        private/* final */GridNode[] nodes;

        public CreateMissingImportedKeyViewJobConf() {} // Externalizable

        CreateMissingImportedKeyViewJobConf(@CheckForNull String viewNamePrefix, @CheckForNull ForeignKey[] fkeys, boolean useGzip, @CheckForNull GridNode[] nodes) {
            if(viewNamePrefix == null) {
                throw new IllegalArgumentException();
            }
            if(fkeys == null || fkeys.length == 0) {
                throw new IllegalArgumentException("ForeignKeys are required: "
                        + Arrays.toString(fkeys));
            }
            if(nodes == null || nodes.length == 0) {
                throw new IllegalArgumentException("Nodes are required: " + Arrays.toString(nodes));
            }
            this.viewNamePrefix = viewNamePrefix;
            this.fkeys = fkeys;
            this.useGzip = useGzip;
            this.nodes = nodes;
        }

        @Nonnull
        public String getViewNamePrefix() {
            return viewNamePrefix;
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
            this.viewNamePrefix = IOUtils.readString(in);
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
            IOUtils.writeString(viewNamePrefix, out);
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
        private/* final */int records;
        private/* final */ForeignKey fkey;
        private/* final */GridNode assocNode;

        @Nullable
        private transient File file;
        @Nullable
        private transient String filePath;

        public DumpFile() {}//Externalizable

        /**
         * @param assocNode one to many
         */
        DumpFile(@Nonnull String fileName, @Nonnull String filePath, @Nonnegative int records, @Nonnull ForeignKey fk, @Nonnull GridNode assocNode) {
            if(records < 1) {
                throw new IllegalArgumentException();
            }
            this.fileName = fileName;
            this.records = records;
            this.fkey = fk;
            this.assocNode = assocNode;
            this.filePath = filePath;
        }

        /**
         * @param assocNode many to one
         */
        DumpFile(@Nonnull File file, @Nonnegative int records, @Nonnull ForeignKey fk, @Nonnull GridNode assocNode) {
            if(records < 1) {
                throw new IllegalArgumentException("Illegal records: " + records);
            }
            this.fileName = file.getName();
            this.records = records;
            this.fkey = fk;
            this.assocNode = assocNode;
            this.file = file;
            this.filePath = file.getAbsolutePath();
        }

        @Nonnull
        String getFileName() {
            return fileName;
        }

        @Nonnegative
        int getRecords() {
            return records;
        }

        @Nonnull
        ForeignKey getForeignKey() {
            return fkey;
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

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.fileName = IOUtils.readString(in);
            this.records = in.readInt();
            this.fkey = (ForeignKey) in.readObject();
            boolean hasAssocNode = in.readBoolean();
            if(hasAssocNode) {
                this.assocNode = (GridNode) in.readObject();
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(fileName, out);
            out.writeInt(records);
            out.writeObject(fkey);
            if(assocNode == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeObject(assocNode);
            }
        }

    }

    public static final class RetrieveMissingForeignKeysJob extends
            GridJobBase<RetrieveMissingForeignKeysJobConf, DumpFile[]> {
        private static final long serialVersionUID = -1419333559953426203L;

        private transient List<DumpFile> dumpedFileList;

        public RetrieveMissingForeignKeysJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, RetrieveMissingForeignKeysJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                GridTask task = new RetrieveMissingForeignKeysTask(this, jobConf);
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

    private static final class RetrieveMissingForeignKeysTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1263155385842261227L;

        private final RetrieveMissingForeignKeysJobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;
        @GridConfigResource
        private transient GridConfiguration config;

        @SuppressWarnings("unchecked")
        RetrieveMissingForeignKeysTask(GridJob job, RetrieveMissingForeignKeysJobConf jobConf) {
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
            final ForeignKey fk = dumpFile.getForeignKey();
            final GridNode origNode = dumpFile.getAssociatedNode();
            final File outputFile = prepareOutputFile(fk, origNode, localNodeId, jobConf.isUseGzip());

            final int affectedRows;
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
            try {
                String importedTableName = prepareTempolaryTable(conn, dumpFile, jobConf.getViewNamePrefix());
                affectedRows = dumpRequiredExportedKeys(conn, fk, importedTableName, outputFile.getAbsolutePath(), registry.getDistributionCatalog());
            } catch (SQLException e) {
                LOG.error(e);
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            if(affectedRows > 0) {
                return new DumpFile(outputFile, affectedRows, fk, origNode);
            } else {
                return null;
            }
        }

        private static File prepareOutputFile(final ForeignKey fk, final GridNode origNode, final String fromNodeId, final boolean gzip)
                throws GridException {
            String toNodeId = getIdentitifier(origNode);
            String outFilePath = WORK_DIR + File.separatorChar + fk.getPkTableName() + ".dump.f"
                    + fromNodeId + 't' + (gzip ? (toNodeId + ".gz") : toNodeId);
            final File outFile = new File(outFilePath);
            if(!outFile.exists()) {
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

        private static String prepareTempolaryTable(final Connection conn, final DumpFile dumpFile, final String viewNamePrefix)
                throws SQLException, GridException {
            // create a table to load a incoming dump file
            ForeignKey fk = dumpFile.getForeignKey();
            String fkName = fk.getFkName();
            GridNode node = dumpFile.getAssociatedNode();
            String tmpTableName = fkName + '_' + getIdentitifier(node);
            String viewName = viewNamePrefix + fkName;
            final String createTable = "CREATE TABLE \"" + tmpTableName + "\" (LIKE \"" + viewName
                    + "\")";
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

        private static int dumpRequiredExportedKeys(final Connection conn, final ForeignKey fk, final String importedTableName, final String outputFilePath, final DistributionCatalog catalog)
                throws GridException, SQLException {
            final StringBuilder queryBuf = new StringBuilder(256);
            queryBuf.append("SELECT l.* FROM \"");
            final String lhsTableName = fk.getPkTableName();
            queryBuf.append(lhsTableName);
            queryBuf.append("\" l, \"");
            queryBuf.append(importedTableName);
            queryBuf.append("\" r WHERE ");
            queryBuf.append(lhsTableName);
            queryBuf.append(" partitioned by (");
            final List<String> pkColumns = fk.getPkColumnNames();
            final int numPkColumns = pkColumns.size();
            for(int i = 0; i < numPkColumns; i++) {
                if(i != 0) {
                    queryBuf.append(',');
                }
                String column = pkColumns.get(i);
                queryBuf.append(column);
            }
            queryBuf.append(")");
            queryBuf.append(" alias l");
            final List<String> fkColumns = fk.getFkColumnNames();
            for(int i = 0; i < numPkColumns; i++) {
                queryBuf.append(" AND l.\"");
                String pkColumn = pkColumns.get(i);
                queryBuf.append(pkColumn);
                queryBuf.append("\" = r.\"");
                String fkColumn = fkColumns.get(i);
                queryBuf.append(fkColumn);
                queryBuf.append('"');
            }
            SQLTranslator trans = new SQLTranslator(catalog);
            String subquery = trans.translateQuery(queryBuf.toString());
            String copyIntoFile = GridDbUtils.makeCopyIntoFileQuery(subquery, outputFilePath);
            return JDBCUtils.update(conn, copyIntoFile);
        }

    }

    static final class RetrieveMissingForeignKeysJobConf implements Externalizable {

        private/* final */DumpFile[] dumpedFiles;
        private/* final */String viewNamePrefix;
        private/* final */GridNode[] nodes;
        private/* final */boolean useGzip;

        public RetrieveMissingForeignKeysJobConf() {}// Externalizable

        RetrieveMissingForeignKeysJobConf(@CheckForNull DumpFile[] dumpedFiles, CreateMissingImportedKeyViewJobConf jobConf) {
            if(dumpedFiles == null) {
                throw new IllegalArgumentException();
            }
            this.dumpedFiles = dumpedFiles;
            this.viewNamePrefix = jobConf.getViewNamePrefix();
            this.nodes = jobConf.getNodes();
            this.useGzip = jobConf.isUseGzip();
        }

        @Nonnull
        public DumpFile[] getDumpedFiles() {
            return dumpedFiles;
        }

        @Nonnull
        public String getViewNamePrefix() {
            return viewNamePrefix;
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
            this.viewNamePrefix = IOUtils.readString(in);
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
            IOUtils.writeString(viewNamePrefix, out);
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
        private final DumpFile[] receivedDumpedFiles;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        ImportCollectedExportedKeysTask(@Nonnull GridJob job, @CheckForNull DumpFile[] receivedDumpedFiles) {
            super(job, false);
            if(receivedDumpedFiles == null) {
                throw new IllegalArgumentException();
            }
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
                Set<ForeignKey> fkeys = loadRequiredRecords(conn, receivedDumpedFiles);
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
            }
            return Boolean.TRUE;
        }

        private static Set<ForeignKey> loadRequiredRecords(final Connection conn, final DumpFile[] dumpedFiles)
                throws SQLException, GridException {
            final Set<ForeignKey> fkeys = new HashSet<ForeignKey>(32);
            for(final DumpFile dumpFile : dumpedFiles) {
                ForeignKey fk = dumpFile.getForeignKey();
                String tableName = fk.getPkTableName();
                String inputFilePath = dumpFile.getFilePath();
                final int expectedRecords = dumpFile.getRecords();
                String copyIntoTable = GridDbUtils.makeCopyIntoTableQuery(tableName, inputFilePath, expectedRecords);
                final int updatedRecords = JDBCUtils.update(conn, copyIntoTable);
                if(expectedRecords != updatedRecords) {
                    throw new GridException("Expected records to import (" + expectedRecords
                            + ") != Actual records imported (" + updatedRecords + "):\n"
                            + copyIntoTable);
                }
                fkeys.add(fk);
            }
            return fkeys;
        }

        private static void addForeignKeyConstraints(final Connection conn, final Set<ForeignKey> fkeys)
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
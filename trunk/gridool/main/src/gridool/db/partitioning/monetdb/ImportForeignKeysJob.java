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
import java.io.Serializable;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
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
        String templateDbName = params.getFirst();
        boolean useGzip = params.getSecond();
        final CreateMissingImportedKeyJobConf jobConf = makeJobConf(templateDbName, useGzip, router);

        // #1 create view for missing foreign keys
        GridJobFuture<Boolean> future1 = kernel.execute(CreateMissingImportedKeyViewJob.class, jobConf);
        GridUtils.invokeGet(future1);

        // #2 ship missing foreign keys and retrieve referencing data
        GridJobFuture<Boolean> future2 = kernel.execute(RetrieveMissingForeignKeysJob.class, jobConf);
        GridUtils.invokeGet(future2);

        return null;
    }

    private CreateMissingImportedKeyJobConf makeJobConf(String templateDbName, boolean useGzip, GridTaskRouter router)
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
        final GridNode[] nodes = router.getAllNodes();
        return new CreateMissingImportedKeyJobConf(viewNamePrefix, fkeyArray, useGzip, nodes);
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return Boolean.TRUE;
    }

    public static final class CreateMissingImportedKeyViewJob extends
            GridJobBase<CreateMissingImportedKeyJobConf, Boolean> {
        private static final long serialVersionUID = -7341912223637268324L;

        public CreateMissingImportedKeyViewJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, CreateMissingImportedKeyJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final int numNodes = nodes.length;
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
            for(final GridNode node : nodes) {
                GridTask task = new CreateMissingImportedKeyViewTask(this, jobConf);
                map.put(task, node);
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            //if(result.getResult() != Boolean.TRUE) {
            //    GridException err = result.getException();
            //    throw new GridException(err);
            //}
            return GridTaskResultPolicy.CONTINUE;
        }

        public Boolean reduce() throws GridException {
            return Boolean.TRUE;
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
        CreateMissingImportedKeyViewTask(@Nonnull GridJob job, @Nonnull CreateMissingImportedKeyJobConf jobConf) {
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

        @Override
        protected Pair<String[], int[]> execute() throws GridException {
            final String query = getCreateViewQuery(fkeys, viewNamePrefix);
            final GridNode localNode = config.getLocalNode();

            DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true);
            final Pair<String[], int[]> dumpList;
            try {
                // #1 create view for missing foreign keys
                JDBCUtils.update(conn, query);
                // #2 dump into file
                dumpList = dumpViewsIntoFiles(conn, fkeys, viewNamePrefix, localNode, useGzip);
            } catch (SQLException e) {
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }

            // #3 scatter dumped file
            int port = config.getFileReceiverPort();
            try {
                scatterDumpedViewFiles(dumpList.getFirst(), port, dstNodes, localNode);
            } catch (IOException e) {
                throw new GridException(e);
            }

            return dumpList;
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

        private static Pair<String[], int[]> dumpViewsIntoFiles(final Connection conn, final ForeignKey[] fkeys, final String viewNamePrefix, final GridNode localNode, final boolean gzip)
                throws SQLException, GridException {
            DbCollection rootCol = DbCollection.getRootCollection();
            File colDir = rootCol.getDirectory();
            final String dirPath = colDir.getAbsolutePath();
            String nodeid = localNode.getPhysicalAdress().getHostAddress() + '_'
                    + localNode.getPort();

            final int numFkeys = fkeys.length;
            final String[] files = new String[numFkeys];
            final int[] rows = new int[numFkeys];
            for(int i = 0; i < numFkeys; i++) {
                ForeignKey fk = fkeys[i];
                String fkName = fk.getFkName();
                String viewName = viewNamePrefix + fkName;
                String filePath = dirPath + File.separatorChar + fkName + ".dump."
                        + (gzip ? (nodeid + ".gz") : nodeid);
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
                String query = GridDbUtils.getCopyIntoFileQuery(subquery, filePath);
                int ret = JDBCUtils.update(conn, query);
                files[i] = filePath;
                rows[i] = ret;
            }
            return new Pair<String[], int[]>(files, rows);
        }

        private static void scatterDumpedViewFiles(final String[] dumpedFiles, final int dstPort, final GridNode[] dstNodes, final GridNode localNode)
                throws IOException {
            for(String filePath : dumpedFiles) {
                for(GridNode node : dstNodes) {
                    if(!node.equals(localNode)) {
                        File file = new File(filePath);
                        if(!file.exists()) {
                            continue;
                        }
                        InetAddress addr = node.getPhysicalAdress();
                        TransferUtils.sendfile(file, addr, dstPort, false, true);
                    }
                }
            }
        }

    }

    static final class CreateMissingImportedKeyJobConf implements Externalizable {

        @Nonnull
        private/* final */String viewNamePrefix;
        @Nonnull
        private/* final */ForeignKey[] fkeys;
        private/* final */boolean useGzip;
        @Nonnull
        private/* final */GridNode[] nodes;

        public CreateMissingImportedKeyJobConf() {} // Externalizable

        CreateMissingImportedKeyJobConf(@CheckForNull String viewNamePrefix, @CheckForNull ForeignKey[] fkeys, boolean useGzip, @CheckForNull GridNode[] nodes) {
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

    public static final class RetrieveMissingForeignKeysJob extends
            GridJobBase<CreateMissingImportedKeyJobConf, Boolean> {
        private static final long serialVersionUID = -1419333559953426203L;

        public RetrieveMissingForeignKeysJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, CreateMissingImportedKeyJobConf jobConf)
                throws GridException {
            final GridNode[] nodes = jobConf.getNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                GridTask task = new RetrieveMissingForeignKeysTask(this, jobConf);
                map.put(task, node);
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            return GridTaskResultPolicy.CONTINUE;
        }

        public Boolean reduce() throws GridException {
            return Boolean.TRUE;
        }

    }

    private static final class RetrieveMissingForeignKeysTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1263155385842261227L;

        private final CreateMissingImportedKeyJobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;
        @GridConfigResource
        private transient GridConfiguration config;

        @SuppressWarnings("unchecked")
        RetrieveMissingForeignKeysTask(GridJob job, CreateMissingImportedKeyJobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Serializable execute() throws GridException {
            DbCollection rootCol = DbCollection.getRootCollection();
            File colDir = rootCol.getDirectory();
            final String dirPath = colDir.getAbsolutePath();
            final boolean gzip = jobConf.isUseGzip();
            final String viewNamePrefix = jobConf.getViewNamePrefix();
            final GridNode localNode = config.getLocalNode();
            final String localNodeId = localNode.getPhysicalAdress().getHostAddress() + '_'
                    + localNode.getPort();
            final int dstPort = config.getFileReceiverPort();

            final ForeignKey[] fkeys = jobConf.getForeignKeys();
            final GridNode[] nodes = jobConf.getNodes();
            for(final ForeignKey fk : fkeys) {
                for(final GridNode node : nodes) {
                    if(!node.equals(localNode)) {
                        String nodeId = node.getPhysicalAdress().getHostAddress() + '_'
                                + node.getPort();
                        String inputFilePath = getInputFilePath(fk, nodeId, dirPath, gzip);
                        if(inputFilePath == null) {
                            continue;
                        }
                        File outputFile = prepareOutputFile(fk, localNodeId, dirPath, gzip);
                        String outFilePath = outputFile.getAbsolutePath();
                        performQuery(inputFilePath, outFilePath, fk, nodeId, viewNamePrefix, registry);
                        InetAddress dstAddr = node.getPhysicalAdress();
                        try {
                            TransferUtils.sendfile(outputFile, dstAddr, dstPort, false, true);
                        } catch (IOException e) {
                            throw new GridException(e);
                        }
                    }
                }
            }
            return null;
        }

        private static String getInputFilePath(final ForeignKey fk, final String nodeid, final String dirPath, final boolean gzip) {
            String inFilePath = dirPath + File.separatorChar + fk.getFkName() + ".dump."
                    + (gzip ? (nodeid + ".gz") : nodeid);
            File inFile = new File(inFilePath);
            if(!inFile.exists()) {
                LOG.warn("File not found: " + inFile.getAbsolutePath());
                return null;
            }
            return inFilePath;
        }

        private static File prepareOutputFile(final ForeignKey fk, final String nodeid, final String dirPath, final boolean gzip)
                throws GridException {
            String outFilePath = dirPath + File.separatorChar + fk.getPkTableName() + ".dump."
                    + (gzip ? (nodeid + ".gz") : nodeid);
            File outFile = new File(outFilePath);
            if(!outFile.exists()) {
                try {
                    if(!outFile.createNewFile()) {
                        throw new GridException("Cannot create a file: "
                                + outFile.getAbsolutePath());
                    }
                } catch (IOException e) {
                    throw new GridException(e);
                }
            }
            return outFile;
        }

        private static void performQuery(final String inFilePath, final String outFilePath, final ForeignKey fk, final String nodeid, final String viewNamePrefix, final GridResourceRegistry registry)
                throws GridException {
            DistributionCatalog catalog = registry.getDistributionCatalog();
            final SQLTranslator trans = new SQLTranslator(catalog);
            final DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
            try {
                String tblName = prepareTempolaryTable(conn, viewNamePrefix, fk, nodeid, inFilePath);
                int ret = dumpMissingForeignKeys(conn, fk, tblName, outFilePath, trans);
                assert (ret > 0);
            } catch (SQLException e) {
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
        }

        private static String prepareTempolaryTable(final Connection conn, final String viewNamePrefix, final ForeignKey fk, final String nodeid, final String inFilePath)
                throws SQLException {
            String fkName = fk.getFkName();
            String tmpTableName = fkName + '_' + nodeid.replace(".", "");
            String viewName = viewNamePrefix + fkName;
            String createTable = "CREATE TABLE \"" + tmpTableName + "\" (LIKE \"" + viewName
                    + "\")";
            JDBCUtils.update(conn, createTable);
            String copyInto = "COPY INTO \"" + tmpTableName + "\" FROM '" + inFilePath
                    + "' USING DELIMITERS '|','\n','\"'";
            int ret = JDBCUtils.update(conn, copyInto);
            if(ret <= 0) {
                LOG.warn("updated ret=" + ret);
            }
            return tmpTableName;
        }

        private static int dumpMissingForeignKeys(final Connection conn, final ForeignKey fk, final String rhsTableName, final String outFilePath, final SQLTranslator trans)
                throws GridException, SQLException {
            String lhsTableName = fk.getPkTableName();
            final StringBuilder queryBuf = new StringBuilder(256);
            queryBuf.append("SELECT l.* FROM \"");
            queryBuf.append(lhsTableName);
            queryBuf.append("\" l, \"");
            queryBuf.append(rhsTableName);
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
            String subquery = trans.translateQuery(queryBuf.toString());
            String sql = GridDbUtils.getCopyIntoFileQuery(subquery, outFilePath);
            return JDBCUtils.update(conn, sql);
        }

    }
}
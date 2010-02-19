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
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
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
        final JobConf jobConf = makeJobConf(templateDbName, useGzip, router);

        // #1 create view for missing foreign keys
        GridJobFuture<Boolean> future1 = kernel.execute(CreateMissingImportedKeyViewJob.class, jobConf);
        GridUtils.invokeGet(future1);

        // #2 ship missing foreign keys and retrieve referrencing data
        GridJobFuture<Boolean> future2 = kernel.execute(RetrieveMissingForeignKeysJob.class, jobConf);
        GridUtils.invokeGet(future2);

        return null;
    }

    private JobConf makeJobConf(String templateDbName, boolean useGzip, GridTaskRouter router)
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
        return new JobConf(viewNamePrefix, fkeyArray, useGzip, nodes);
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return Boolean.TRUE;
    }

    public static final class CreateMissingImportedKeyViewJob extends GridJobBase<JobConf, Boolean> {
        private static final long serialVersionUID = -7341912223637268324L;

        public CreateMissingImportedKeyViewJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf)
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
            if(result.getResult() != Boolean.TRUE) {
                GridException err = result.getException();
                throw new GridException(err);
            }
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
        CreateMissingImportedKeyViewTask(@Nonnull GridJob job, @Nonnull JobConf jobConf) {
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
                scatterDumpedFiles(dumpList.getFirst(), port, dstNodes, localNode);
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
                    buf.append("l.\"");
                    String fkc = fkColumns.get(i);
                    buf.append(fkc);
                    buf.append("\" IS NULL");
                }
                buf.append("\n);\n");
            }
            return buf.toString();
        }

        private static Pair<String[], int[]> dumpViewsIntoFiles(final Connection conn, final ForeignKey[] fkeys, final String viewNamePrefix, final GridNode localNode, final boolean gzip)
                throws SQLException {
            DbCollection rootCol = DbCollection.getRootCollection();
            File colDir = rootCol.getDirectory();
            final String dirPath = colDir.getAbsolutePath();
            String nodeid = GridUtils.getNodeIdentityNumber(localNode);

            final int numFkeys = fkeys.length;
            final String[] files = new String[numFkeys];
            final int[] rows = new int[numFkeys];
            final Pair<String[], int[]> list = new Pair<String[], int[]>(files, rows);
            for(int i = 0; i < numFkeys; i++) {
                ForeignKey fk = fkeys[i];
                String fkName = fk.getFkName();
                String viewName = viewNamePrefix + fkName;
                String filePath = dirPath + File.separatorChar + fkName + ".dump."
                        + (gzip ? (nodeid + ".gz") : nodeid);
                String subquery = "SELECT * FROM \"" + viewName + '"';
                String query = GridDbUtils.getCopyIntoFileQuery(subquery, filePath);
                int ret = JDBCUtils.update(conn, query);
                files[i] = filePath;
                rows[i] = ret;
            }
            return list;
        }

        private static void scatterDumpedFiles(final String[] dumpedFiles, final int dstPort, final GridNode[] dstNodes, final GridNode localNode)
                throws IOException {
            for(String fp : dumpedFiles) {
                for(GridNode node : dstNodes) {
                    if(!node.equals(localNode)) {
                        File file = new File(fp);
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

    public static final class RetrieveMissingForeignKeysJob extends GridJobBase<JobConf, Boolean> {
        private static final long serialVersionUID = -1419333559953426203L;

        @GridConfigResource
        private transient GridConfiguration config;

        public RetrieveMissingForeignKeysJob() {
            super();
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf)
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

        private final JobConf jobConf;

        @GridConfigResource
        private transient GridConfiguration config;

        @SuppressWarnings("unchecked")
        RetrieveMissingForeignKeysTask(GridJob job, JobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        protected Serializable execute() throws GridException {
            final ForeignKey[] fkeys = jobConf.getForeignKeys();

            return null;
        }

    }

    static final class JobConf implements Externalizable {

        @Nonnull
        private/* final */String viewNamePrefix;
        @Nonnull
        private/* final */ForeignKey[] fkeys;
        private/* final */boolean useGzip;
        @Nonnull
        private/* final */GridNode[] nodes;

        public JobConf() {} // Externalizable

        JobConf(@CheckForNull String viewNamePrefix, @CheckForNull ForeignKey[] fkeys, boolean useGzip, @CheckForNull GridNode[] nodes) {
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

}
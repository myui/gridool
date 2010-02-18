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
package gridool.tools.cmd;

import gridool.Grid;
import gridool.GridClient;
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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import xbird.util.cmdline.CommandBase;
import xbird.util.cmdline.CommandException;
import xbird.util.cmdline.Option.StringOption;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.lang.ArrayUtils;

/**
 * -templateDb <dbname> import foreign keys
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ImportForeignKeysCommand extends CommandBase {

    public ImportForeignKeysCommand() {
        addOption(new StringOption("templateDb", "jdbc:monetdb://localhost/templatedb", true));
    }

    public boolean match(String[] args) {
        if(args.length != 3) {
            return false;
        }
        if(!"import".equalsIgnoreCase(args[0])) {
            return false;
        }
        if("foreign".equalsIgnoreCase(args[1])) {
            return false;
        }
        if("keys".equalsIgnoreCase(args[2])) {
            return false;
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        final String templateDbName = getOption("templateDb");
        final Grid grid = new GridClient();
        try {
            grid.execute(ImportForeignKeysJob.class, templateDbName);
        } catch (RemoteException e) {
            throwException(e.getMessage());
            return false;
        }
        return true;
    }

    public String usage() {
        return constructHelp("Import ", "-templateDb <dbname> import foreign keys");
    }

    static final class ImportForeignKeysJob extends GridJobBase<String, Boolean> {
        private static final long serialVersionUID = -1580857354649767246L;

        @GridKernelResource
        private GridKernel kernel;

        @GridRegistryResource
        private GridResourceRegistry registry;

        public ImportForeignKeysJob() {
            super();
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, String templateDbName)
                throws GridException {
            final JobConf jobConf = makeJobConf(templateDbName, router);

            // #1 create view for missing foreign keys
            GridJobFuture<Boolean> future1 = kernel.execute(CreateMissingImportedKeyViewJob.class, jobConf);
            GridUtils.invokeGet(future1);

            // #2 ship missing foreign keys and retrieve data
            GridJobFuture<Boolean> future2 = kernel.execute(RetrieveMissingForeignKeysJob.class, jobConf);
            GridUtils.invokeGet(future2);

            return null;
        }

        private JobConf makeJobConf(String templateDbName, GridTaskRouter router)
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
            return new JobConf(viewNamePrefix, fkeyArray, nodes);
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            return null;
        }

        public Boolean reduce() throws GridException {
            return null;
        }

    }

    static final class CreateMissingImportedKeyViewJob extends GridJobBase<JobConf, Boolean> {
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

        @GridRegistryResource
        private GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected CreateMissingImportedKeyViewTask(@Nonnull GridJob job, @Nonnull JobConf jobConf) {
            super(job, false);
            this.fkeys = jobConf.getForeignKeys();
            this.viewNamePrefix = jobConf.getViewNamePrefix();
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Serializable execute() throws GridException {
            final String query = getCreateViewQuery(fkeys, viewNamePrefix);

            // create view for missing foreign keys
            DBAccessor dba = registry.getDbAccessor();
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true);
            try {
                JDBCUtils.update(conn, query);
            } catch (SQLException e) {
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }

            return Boolean.TRUE;
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
                if(numFkColumns == numPkColumns) {
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
                buf.append("\n)");
            }
            return buf.toString();
        }

    }

    static final class RetrieveMissingForeignKeysJob extends GridJobBase<JobConf, Boolean> {
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
            GridNode localNode = config.getLocalNode();
            final String nodeid = GridUtils.getNodeIdentityNumber(localNode);

            final GridNode[] nodes = jobConf.getNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                
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
        private final InetAddress dstAddr;
        private final int dstPort;

        @SuppressWarnings("unchecked")
        protected RetrieveMissingForeignKeysTask(GridJob job, JobConf jobConf, @Nonnull InetSocketAddress retSockAddr) {
            super(job, false);
            this.jobConf = jobConf;
            this.dstAddr = retSockAddr.getAddress();
            this.dstPort = retSockAddr.getPort();
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
        @Nonnull
        private/* final */GridNode[] nodes;

        public JobConf() {} // Externalizable

        JobConf(@CheckForNull String viewNamePrefix, @CheckForNull ForeignKey[] fkeys, @CheckForNull GridNode[] nodes) {
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
            out.writeInt(nodes.length);
            for(int i = 0; i < nodes.length; i++) {
                out.writeObject(nodes[i]);
            }
        }

    }

}

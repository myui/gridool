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
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.replication.ReplicationManager;
import gridool.routing.GridRouter;
import gridool.util.cmdline.CommandBase;
import gridool.util.cmdline.CommandException;
import gridool.util.cmdline.Option.StringOption;
import gridool.util.io.IOUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.lang.ArrayUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * register [cluster] replica DBNAME1 DBNAME2 .. DBNAMEn
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class RegisterReplicaDbCommand extends CommandBase {
    private static final Log LOG = LogFactory.getLog(RegisterReplicaDbCommand.class);

    public RegisterReplicaDbCommand() {
        super();
        addOption(new StringOption("driverClassName", "nl.cwi.monetdb.jdbc.MonetDriver", true));
        addOption(new StringOption("primaryDbUrl", true));
        addOption(new StringOption("user", true));
        addOption(new StringOption("passwd", true));
    }

    public boolean match(String[] args) {
        if(args.length < 3) {
            return false;
        }
        if(!"register".equalsIgnoreCase(args[0])) {
            return false;
        }
        if("cluster".equalsIgnoreCase(args[1])) {
            if(args.length < 4) {
                return false;
            }
            if(!"replica".equalsIgnoreCase(args[2])) {
                return false;
            }
        } else {
            if(!"replica".equalsIgnoreCase(args[1])) {
                return false;
            }
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        String driverClassName = getOption("driverClassName");
        String primaryDbUrl = getOption("primaryDbUrl");
        String user = getOption("user");
        String passwd = getOption("passwd");
        final boolean isLocal = !"cluster".equalsIgnoreCase(args[1]);
        final String[] dbnames;
        if(isLocal) {
            dbnames = ArrayUtils.copyOfRange(args, 2, args.length);
        } else {
            dbnames = ArrayUtils.copyOfRange(args, 3, args.length);
        }

        final JobConf jobConf = new JobConf(driverClassName, primaryDbUrl, user, passwd, dbnames, isLocal);
        final Grid grid = new GridClient();
        final Boolean succeed;
        try {
            succeed = grid.execute(RegisterReplicaDbJob.class, jobConf);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        return (succeed == null) ? false : succeed.booleanValue();
    }

    public String usage() {
        return constructHelp("Register replica databases", "register replica DBNAME1 DBNAME2 .. DBNAMEn");
    }

    public static final class RegisterReplicaDbJob extends GridJobBase<JobConf, Boolean> {
        private static final long serialVersionUID = 375880295535375239L;

        private transient boolean succeed = true;

        public RegisterReplicaDbJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridRouter router, JobConf jobConf)
                throws GridException {
            if(jobConf.isLocalTask) {
                return localMap(jobConf);
            } else {
                return clusterMap(router, jobConf);
            }
        }

        private Map<GridTask, GridNode> localMap(JobConf jobConf) {
            GridNode localNode = getJobNode();
            Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
            GridTask task = new RegisterReplicaDbTask(this, jobConf);
            map.put(task, localNode);
            return map;
        }

        private Map<GridTask, GridNode> clusterMap(GridRouter router, JobConf jobConf) {
            final GridNode[] nodes = router.getAllNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                GridTask task = new RegisterReplicaDbTask(this, jobConf);
                map.put(task, node);
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            Boolean res = result.getResult();
            if(res == null || !res.booleanValue()) {
                if(LOG.isWarnEnabled()) {
                    LOG.warn("RegisterReplicaTask failed on node: " + result.getExecutedNode());
                }
                this.succeed = false;
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public Boolean reduce() throws GridException {
            return succeed;
        }
    }

    private static final class RegisterReplicaDbTask extends GridTaskAdapter {
        private static final long serialVersionUID = 7982567601211289995L;

        private final JobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected RegisterReplicaDbTask(GridJob job, JobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        protected Boolean execute() throws GridException {
            final ReplicationManager repManager = registry.getReplicationManager();

            final Connection conn;
            try {
                conn = JDBCUtils.getConnection(jobConf.primaryDbUrl, jobConf.driverClassName, jobConf.user, jobConf.passwd);
                conn.setAutoCommit(false);
            } catch (ClassNotFoundException e) {
                LOG.error(e);
                return false;
            } catch (SQLException sqle) {
                LOG.error(sqle);
                return false;
            }

            final boolean suceed;
            try {
                suceed = repManager.registerReplicaDatabase(conn, jobConf.dbnames);
            } catch (SQLException e) {
                LOG.warn(e);
                return false;
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            return suceed;
        }

    }

    static final class JobConf implements Externalizable {
        private static final long serialVersionUID = 7604873494442633959L;

        private String driverClassName;
        private String primaryDbUrl;
        private String user;
        private String passwd;
        private String[] dbnames;
        private boolean isLocalTask;

        public JobConf() {}// for Externalizable

        public JobConf(String driverClassName, String primaryDbUrl, String user, String passwd, String[] dbnames, boolean isLocal) {
            checkArgs(driverClassName, primaryDbUrl, dbnames);
            this.driverClassName = driverClassName;
            this.primaryDbUrl = primaryDbUrl;
            this.user = user;
            this.passwd = passwd;
            this.dbnames = dbnames;
            this.isLocalTask = isLocal;
        }

        private static void checkArgs(String driverClassName, String primaryDbUrl, String[] dbnames) {
            if(driverClassName == null) {
                throw new IllegalArgumentException();
            }
            if(primaryDbUrl == null) {
                throw new IllegalArgumentException();
            }
            if(dbnames == null || dbnames.length == 0) {
                throw new IllegalArgumentException();
            }
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.driverClassName = IOUtils.readString(in);
            this.primaryDbUrl = IOUtils.readString(in);
            this.user = IOUtils.readString(in);
            this.passwd = IOUtils.readString(in);
            final int numdbs = in.readInt();
            this.dbnames = new String[numdbs];
            for(int i = 0; i < numdbs; i++) {
                dbnames[i] = IOUtils.readString(in);
            }
            this.isLocalTask = in.readBoolean();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(driverClassName, out);
            IOUtils.writeString(primaryDbUrl, out);
            IOUtils.writeString(user, out);
            IOUtils.writeString(passwd, out);
            final int numdbs = dbnames.length;
            out.writeInt(numdbs);
            for(int i = 0; i < numdbs; i++) {
                IOUtils.writeString(dbnames[i], out);
            }
            out.writeBoolean(isLocalTask);
        }

    }

}

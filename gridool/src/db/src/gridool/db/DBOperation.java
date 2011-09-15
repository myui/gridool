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
package gridool.db;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.replication.ReplicationManager;
import gridool.util.io.IOUtils;
import gridool.util.jdbc.JDBCUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DBOperation implements Externalizable {
    private static final long serialVersionUID = -4228703722773423150L;

    @Nonnull
    protected/* final */String driverClassName;
    @Nonnull
    protected/* final */String connectUrl;
    private transient String replicaConnectUrl = null;
    @Nullable
    protected String userName = null;
    @Nullable
    protected String password = null;

    @Nullable
    private GridNode masterNode = null;
    private boolean transferredForReplica = false;

    @Nullable
    protected transient GridResourceRegistry registry;

    public DBOperation() {}// Externalizable

    public DBOperation(@CheckForNull String driverClassName, @CheckForNull String connectUrl) {
        if(driverClassName == null) {
            throw new IllegalArgumentException("Driver class must be specified");
        }
        if(connectUrl == null) {
            throw new IllegalArgumentException("Connect Url must be specified");
        }
        this.driverClassName = driverClassName;
        this.connectUrl = connectUrl;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getConnectUrl() {
        return connectUrl;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public final void setAuth(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public boolean isReplicatable() {
        return true;
    }

    public void setTransferToReplica(@Nonnull GridNode masterNode) {
        this.masterNode = masterNode;
    }

    @Nullable
    protected final GridNode getMasterNode() {
        return masterNode;
    }

    public void setResourceRegistry(@Nonnull GridResourceRegistry registry) {
        this.registry = registry;
    }

    /**
     * Must be called after {@link #getConnection()}
     */
    public final String getConnectionUrl() {
        if(transferredForReplica) {
            return replicaConnectUrl;
        } else {
            return connectUrl;
        }
    }

    public final Connection getConnection() throws SQLException, GridException {
        final Connection conn;
        try {
            conn = JDBCUtils.getConnection((replicaConnectUrl != null) ? replicaConnectUrl
                    : connectUrl, driverClassName, userName, password);
        } catch (ClassNotFoundException e) {
            throw new GridException("Failed to get a database connection: " + connectUrl, e);
        }
        configureConnection(conn);
        if(transferredForReplica && replicaConnectUrl == null) {
            final String replicaUrl;
            try {
                replicaUrl = getReplicaDbUrl(conn, masterNode);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            this.replicaConnectUrl = replicaUrl;
            Connection replicaConn = JDBCUtils.getConnection(replicaUrl, userName, password);
            configureConnection(replicaConn);
            return replicaConn;
        }
        return conn;
    }

    protected void configureConnection(@Nonnull Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    private String getReplicaDbUrl(@Nonnull Connection conn, @Nonnull GridNode masterNode)
            throws SQLException {
        if(registry == null) {
            throw new IllegalStateException("GridResourceRegistory is not set");
        }
        ReplicationManager replicationMgr = registry.getReplicationManager();
        String replicaDbName = replicationMgr.getReplicaDatabaseName(conn, masterNode);
        if(replicaDbName == null) {
            throw new IllegalStateException("Replica database of node '" + masterNode
                    + "' does not exist");
        }
        // REVIEWME monetdb specific code
        String localdbUrl = connectUrl.substring(0, connectUrl.lastIndexOf('/') + 1);
        String dbUrl = localdbUrl + replicaDbName;
        return dbUrl;
    }

    public abstract Serializable execute() throws SQLException, GridException;

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(driverClassName, out);
        IOUtils.writeString(connectUrl, out);
        IOUtils.writeString(userName, out);
        IOUtils.writeString(password, out);
        out.writeObject(masterNode);
        out.writeBoolean(masterNode != null); // transferred
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.driverClassName = IOUtils.readString(in);
        this.connectUrl = IOUtils.readString(in);
        this.userName = IOUtils.readString(in);
        this.password = IOUtils.readString(in);
        this.masterNode = (GridNode) in.readObject();
        this.transferredForReplica = in.readBoolean();
    }
}

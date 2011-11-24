/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.sqlet;

import gridool.GridNode;
import gridool.sqlet.catalog.PartitioningConf;
import gridool.util.GridUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.jdbc.ResultSetHandler;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class SqletConf implements Serializable {
    private static final long serialVersionUID = -5675434907243377035L;

    @Nonnull
    private final PartitioningConf partitions;
    @Nonnull
    private final List<ReducerInfo> reducers;

    public SqletConf(@Nonnull PartitioningConf partitions) {
        this.partitions = partitions;
        this.reducers = new ArrayList<ReducerInfo>(4);
    }

    @Nonnull
    public PartitioningConf getPartitions() {
        return partitions;
    }

    @Nonnull
    public List<ReducerInfo> getReducers() {
        return reducers;
    }

    public void addReducer(@Nonnull GridNode host, @Nonnull String dbUrl) {
        int seqid = reducers.size() + 1;
        addReducer(seqid, host, dbUrl, null);
    }

    public void addReducer(int id, @Nonnull GridNode host, @Nonnull String dbUrl, @Nullable String shuffleDataSink) {
        ReducerInfo r = new ReducerInfo(id, host, dbUrl, shuffleDataSink);
        reducers.add(r);
    }
    
    public void loadReduceSettings(File file) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }
    
    public void loadReduceSettings(Connection conn, String tblName) throws SQLException {
        String sql = "SELECT id, host, port, dbUrl, shuffleDataSink FROM " + tblName;
        ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    int id = rs.getInt(1);
                    String host = rs.getString(2);
                    int port = rs.getInt(3);
                    GridNode hostNode = GridUtils.getNode(host, port);
                    String dbUrl = rs.getString(4);
                    String shuffleDataSink = rs.getString(5);
                    ReducerInfo r = new ReducerInfo(id, hostNode, dbUrl, shuffleDataSink);
                    reducers.add(r);
                }
                return null;
            }
        };
        JDBCUtils.query(conn, sql, rsh);
    }

    public static final class ReducerInfo implements Serializable {
        private static final long serialVersionUID = -8888959454036316181L;

        final int id;
        @Nonnull
        final GridNode host;
        @Nonnull
        final String dbUrl;
        @Nullable
        final String shuffleDataSink;

        ReducerInfo(int id, @Nonnull GridNode host, @Nonnull String dbUrl, @Nullable String shuffleDataSink) {
            this.id = id;
            this.host = host;
            this.dbUrl = dbUrl;
            this.shuffleDataSink = shuffleDataSink;
        }

    }

}

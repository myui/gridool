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
package gridool.db.partitioning;

import gridool.GridTask;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.jdbc.JDBCUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBPartitioningJobConf implements Serializable {
    private static final long serialVersionUID = 636573640789390674L;

    private transient Pair<int[], int[]> partitionigKeyIndices = null;

    public DBPartitioningJobConf() {}

    // -------------------------------------------------------------
    // CSV stuffs

    @Nonnull
    public abstract String getCsvFilePath();

    @Nonnull
    public abstract String getTableName();

    /**
     * Returns pair of primary key column indices and foreign key column indices.
     * Recommended to override if possible.
     * 
     * @see #getBaseTableName()
     */
    @Nonnull
    public synchronized Pair<int[], int[]> partitionigKeyIndices() {
        if(partitionigKeyIndices == null) {
            final String tblName = getBaseTableName();
            try {
                final Connection conn = getConnection(false);
                try {
                    partitionigKeyIndices = JDBCUtils.getPartitioningKeys(conn, tblName);
                } finally {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        ;
                    }
                }
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalStateException(cnfe);
            } catch (SQLException sqle) {
                throw new IllegalStateException(sqle);
            }
        }
        return partitionigKeyIndices;
    }
    
    @Nonnull
    protected String getBaseTableName() {
        throw new UnsupportedOperationException();
    }

    public char getFieldSeparator() {
        return '\t';
    }

    public String getRecordSeparator() {
        return "\n";
    }

    public char getStringQuote() {
        return '\"';
    }

    // -------------------------------------------------------------
    // JDBC stuffs

    @Nonnull
    public abstract String getDriverClassName();

    @Nonnull
    public abstract String getConnectUrl();

    @Nullable
    public String getUserName() {
        return null;
    }

    @Nullable
    public String getPassword() {
        return null;
    }

    @Nonnull
    public abstract String getCreateTableDDL();

    @Nullable
    public String getAlterTableDDL() {
        return null;
    }

    @Nonnull
    public final Connection getConnection(boolean autoCommit) throws ClassNotFoundException,
            SQLException {
        Class.forName(getDriverClassName());
        final String url = getConnectUrl();
        final String user = getUserName();
        final Connection conn;
        if(user == null) {
            conn = DriverManager.getConnection(url);
        } else {
            String password = getPassword();
            conn = DriverManager.getConnection(url, user, password);
        }
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    @Nonnull
    public abstract GridTask makePartitioningTask(DBPartitioningJob job);

}

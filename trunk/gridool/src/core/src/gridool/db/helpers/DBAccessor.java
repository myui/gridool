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
package gridool.db.helpers;

import gridool.Settings;
import gridool.util.GridUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.jdbc.PooledDbConnection;
import gridool.util.pool.BoundedObjectPool;
import gridool.util.pool.ObjectPool;

import java.sql.Connection;
import java.sql.SQLException;

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
public class DBAccessor {
    private static final Log LOG = LogFactory.getLog(DBAccessor.class);

    public static final String SYS_TABLE_SYMBOL = "__i_am_system_table__";

    protected static final String PROP_DRIVER = "gridool.db.driver";
    protected static final String PROP_DBURL = "gridool.db.primarydb.url";
    protected static final String PROP_USER = "gridool.db.user";
    protected static final String PROP_PASSWORD = "gridool.db.password";

    @Nonnull
    private static final String driverClassName;
    @Nonnull
    private static final String primaryDbUrl;
    @Nonnull
    private static final String userName;
    @Nonnull
    private static final String password;
    @Nonnull
    private final String dbUrlPrefix;
    @Nonnull
    private final String primaryDbName;

    @Deprecated
    private transient final ObjectPool<PooledDbConnection> primaryDbConnPool;

    static {
        driverClassName = Settings.get(PROP_DRIVER, "nl.cwi.monetdb.jdbc.MonetDriver");
        primaryDbUrl = Settings.get(PROP_DBURL);
        userName = Settings.get(PROP_USER);
        password = Settings.get(PROP_PASSWORD);
    }

    public DBAccessor() {
        this.dbUrlPrefix = extractDbPrefix(primaryDbUrl);
        this.primaryDbName = extractDbName(primaryDbUrl);
        this.primaryDbConnPool = new BoundedObjectPool<PooledDbConnection>(3, false) {
            @Override
            protected PooledDbConnection createObject() {
                final Connection origConn;
                try {
                    origConn = JDBCUtils.getConnection(primaryDbUrl, driverClassName, userName, password);
                } catch (ClassNotFoundException ce) {
                    LOG.error(ce);
                    throw new IllegalStateException(ce);
                } catch (SQLException se) {
                    LOG.error(se);
                    throw new IllegalStateException(se);
                }
                return new PooledDbConnection(origConn, this);
            }

            @Override
            public PooledDbConnection borrowObject() {
                final PooledDbConnection pooled = queue.poll();
                if(pooled != null) {
                    try {
                        if(!pooled.isClosed()) {
                            return pooled;
                        }
                    } catch (SQLException e) {
                        ;
                    }
                }
                PooledDbConnection created = createObject();
                return created;
            }

            @Override
            public boolean returnObject(PooledDbConnection conn) {
                if(queue.offer(conn)) {
                    return true;
                }
                close(conn);
                return false;
            }

            @Override
            public void clear() {
                PooledDbConnection conn = queue.poll();
                while(conn != null) {
                    close(conn);
                    conn = queue.poll();
                }
            }

            private void close(PooledDbConnection conn) {
                Connection realConn = conn.getDbConnection();
                JDBCUtils.closeQuietly(realConn);
            }
        };
    }

    public String getPrimaryDbName() {
        return primaryDbName;
    }

    public Connection getConnection(@Nonnull String dbName) throws SQLException {
        final Connection conn;
        if(primaryDbName.equalsIgnoreCase(dbName)) {
            conn = getPrimaryDbConnection();
        } else {
            final String replicaDbUrl = dbUrlPrefix + dbName;
            try {
                conn = JDBCUtils.getConnection(replicaDbUrl, driverClassName, userName, password);
            } catch (ClassNotFoundException e) {
                throw new SQLException(e);
            }
        }
        return conn;
    }

    public Connection getPrimaryDbConnection() throws SQLException {
        try {
            return JDBCUtils.getConnection(primaryDbUrl, driverClassName, userName, password);
        } catch (ClassNotFoundException ce) {
            LOG.error(ce);
            throw new SQLException(ce);
        }
    }

    public void releasePooledConnections() {
        primaryDbConnPool.clear();
    }

    protected String extractDbPrefix(@Nonnull String dburl) {
        try {
            return dburl.substring(0, dburl.lastIndexOf('/') + 1);
        } catch (StringIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid DB url: " + dburl);
        }
    }

    protected String extractDbName(@Nonnull String dburl) {
        return GridUtils.extractDbName(dburl);
    }

}

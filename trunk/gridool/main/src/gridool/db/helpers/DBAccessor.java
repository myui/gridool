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

import gridool.util.GridUtils;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.jdbc.JDBCUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class DBAccessor {
    private static final Log LOG = LogFactory.getLog(DBAccessor.class);

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

    static {
        driverClassName = Settings.get(PROP_DRIVER, "nl.cwi.monetdb.jdbc.MonetDriver");
        primaryDbUrl = Settings.get(PROP_DBURL);
        userName = Settings.get(PROP_USER);
        password = Settings.get(PROP_PASSWORD);
    }

    public DBAccessor() {
        this.dbUrlPrefix = extractDbPrefix(primaryDbUrl);
        this.primaryDbName = extractDbName(primaryDbUrl);
    }

    public String getPrimaryDbName() {
        return primaryDbName;
    }

    public Connection getConnection(@Nonnull String dbName) throws SQLException {
        final Connection conn;
        if(primaryDbName.equalsIgnoreCase(dbName)) {
            try {
                conn = JDBCUtils.getConnection(primaryDbUrl, driverClassName, userName, password);
            } catch (ClassNotFoundException e) {
                throw new SQLException(e);
            }
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
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
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

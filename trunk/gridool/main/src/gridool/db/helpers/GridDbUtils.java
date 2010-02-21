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

import gridool.GridException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridDbUtils {
    private static final Log LOG = LogFactory.getLog(GridDbUtils.class);

    private GridDbUtils() {}

    @Nonnull
    public static Connection getPrimaryDbConnection(final DBAccessor dba, boolean autoCommit)
            throws GridException {
        final Connection conn;
        try {
            conn = dba.getPrimaryDbConnection();
            if(conn.getAutoCommit() != autoCommit) {
                conn.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException("failed connecting to the primary database: "
                    + dba.getPrimaryDbName());
        }
        return conn;
    }

    @Nullable
    public static PrimaryKey getPrimaryKey(@Nonnull final Connection conn, @CheckForNull final String tableName)
            throws SQLException {
        if(tableName == null) {
            throw new IllegalArgumentException();
        }
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();

        final ResultSet rs = metadata.getPrimaryKeys(catalog, null, tableName);
        if(!rs.next()) {
            return null;
        }
        String pkName = rs.getString("PK_NAME");
        final PrimaryKey pkey = new PrimaryKey(pkName, tableName, true);
        do {
            pkey.addColumn(rs, metadata);
        } while(rs.next());
        return pkey;
    }

    @Nonnull
    public static Collection<ForeignKey> getForeignKeys(@Nonnull final Connection conn, @CheckForNull final String tableName)
            throws SQLException {
        if(tableName == null) {
            throw new IllegalArgumentException();
        }
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();

        final Map<String, ForeignKey> mapping = new HashMap<String, ForeignKey>(4);
        final ResultSet rs = metadata.getImportedKeys(catalog, null, tableName);
        while(rs.next()) {
            final String fkName = rs.getString("FK_NAME");
            ForeignKey fk = mapping.get(fkName);
            if(fk == null) {
                String fkTableName = rs.getString("FKTABLE_NAME");
                String pkTableName = rs.getString("PKTABLE_NAME");
                fk = new ForeignKey(fkName, fkTableName, pkTableName, true);
                mapping.put(fkName, fk);
            }
            fk.addColumn(rs, metadata);
        }
        return mapping.values();
    }

    public static String makeCopyIntoFileQuery(final String subquery, final String outputFilePath) {
        return "COPY (" + subquery + ") INTO '" + outputFilePath
                + "' USING DELIMITERS '|','\n','\"'";
    }

    public static String makeCopyIntoTableQuery(final String tableName, final String inputFilePath, final int expectedRecords) {
        if(expectedRecords <= 0) {
            throw new IllegalArgumentException("Illegal expected records: " + expectedRecords);
        }
        return "COPY " + expectedRecords + " RECORDS INTO \"" + tableName + "\" FROM '"
                + inputFilePath + "' USING DELIMITERS '|','\n','\"'";
    }

}

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    public static PrimaryKey getPrimaryKey(@Nonnull final Connection conn, @CheckForNull final String pkTableName, final boolean reserveAdditionalInfo)
            throws SQLException {
        if(pkTableName == null) {
            throw new IllegalArgumentException();
        }
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final ResultSet rs = metadata.getPrimaryKeys(catalog, null, pkTableName);
        final PrimaryKey pkey;
        try {
            if(!rs.next()) {
                return null;
            }
            String pkName = rs.getString("PK_NAME");
            pkey = new PrimaryKey(pkName, pkTableName);
            do {
                pkey.addColumn(rs);
            } while(rs.next());
        } finally {
            rs.close();
        }

        if(reserveAdditionalInfo) {
            // set foreign key column positions
            pkey.setColumnPositions(metadata);
            // set exported keys
            final Collection<ForeignKey> exportedKeys = getExportedKeys(conn, pkTableName, false);
            if(!exportedKeys.isEmpty()) {
                final List<String> pkColumnsProbe = pkey.getColumnNames();
                final int numPkColumnsProbe = pkColumnsProbe.size();
                final List<ForeignKey> exportedKeyList = new ArrayList<ForeignKey>(4);
                outer: for(ForeignKey fk : exportedKeys) {
                    List<String> names = fk.getPkColumnNames();
                    if(names.size() != numPkColumnsProbe) {
                        continue;
                    }
                    for(String name : names) {
                        if(!pkColumnsProbe.contains(name)) {
                            continue outer;
                        }
                    }
                    exportedKeyList.add(fk);
                }
                if(!exportedKeyList.isEmpty()) {
                    pkey.setExportedKeys(exportedKeyList);
                }
            }
        }
        return pkey;
    }

    /**
     * @return column position is not provided in the returning foreign keys
     */
    @Nonnull
    public static Collection<ForeignKey> getExportedKeys(@Nonnull final Connection conn, @Nullable final String pkTableName, final boolean setColumnPositions)
            throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final Map<String, ForeignKey> mapping = new HashMap<String, ForeignKey>(4);
        final ResultSet rs = metadata.getExportedKeys(catalog, null, pkTableName);
        try {
            while(rs.next()) {
                final String fkName = rs.getString("FK_NAME");
                ForeignKey fk = mapping.get(fkName);
                if(fk == null) {
                    String fkTableName = rs.getString("FKTABLE_NAME");
                    fk = new ForeignKey(fkName, fkTableName, pkTableName);
                    mapping.put(fkName, fk);
                }
                fk.addColumn(rs, metadata);
            }
        } finally {
            rs.close();
        }
        final Collection<ForeignKey> fkeys = mapping.values();
        if(setColumnPositions) {
            for(ForeignKey fk : fkeys) {
                fk.setColumnPositions(metadata);
            }
        }
        return fkeys;
    }

    /**
     * @return column position is provided in the returning foreign keys
     */
    @Nonnull
    public static Collection<ForeignKey> getForeignKeys(@Nonnull final Connection conn, @Nullable final String fkTableName, final boolean setColumnPositions)
            throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final Map<String, ForeignKey> mapping = new HashMap<String, ForeignKey>(4);
        final ResultSet rs = metadata.getImportedKeys(catalog, null, fkTableName);
        try {
            while(rs.next()) {
                final String fkName = rs.getString("FK_NAME");
                ForeignKey fk = mapping.get(fkName);
                if(fk == null) {
                    //String fkTableName = rs.getString("FKTABLE_NAME");
                    String pkTableName = rs.getString("PKTABLE_NAME");
                    fk = new ForeignKey(fkName, fkTableName, pkTableName);
                    mapping.put(fkName, fk);
                }
                fk.addColumn(rs, metadata);
            }
        } finally {
            rs.close();
        }
        final Collection<ForeignKey> fkeys = mapping.values();
        if(setColumnPositions) {
            for(ForeignKey fk : fkeys) {
                fk.setColumnPositions(metadata);
            }
        }
        return fkeys;
    }

    public static String getCombinedColumnName(final List<String> columnNames, final boolean sortByName) {
        final int size = columnNames.size();
        if(size == 0) {
            throw new IllegalArgumentException();
        }
        if(size == 1) {
            return columnNames.get(0);
        }
        if(sortByName) {
            Collections.sort(columnNames);
        }
        final StringBuilder buf = new StringBuilder(32);
        for(int i = 0; i < size; i++) {
            if(i != 0) {
                buf.append('&');
            }
            String name = columnNames.get(i);
            buf.append(name);
        }
        return buf.toString();
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

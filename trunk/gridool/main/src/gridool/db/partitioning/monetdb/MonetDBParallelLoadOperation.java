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

import gridool.db.DBOperation;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBParallelLoadOperation extends DBOperation {
    private static final long serialVersionUID = 2815346044185945907L;
    private static final Log LOG = LogFactory.getLog(MonetDBParallelLoadOperation.class);

    @Nonnull
    private/* final */String tableName;
    @Nonnull
    private/* final */String createTableDDL;
    @Nonnull
    private/* final */String copyIntoQuery;

    public MonetDBParallelLoadOperation() {}

    public MonetDBParallelLoadOperation(String driverClassName, String connectUrl, @Nonnull String tableName, @Nonnull String createTableDDL, @Nonnull String copyIntoQuery) {
        super(driverClassName, connectUrl);
    }

    public String getTableName() {
        return tableName;
    }

    public String getCreateTableDDL() {
        return createTableDDL;
    }

    public String getCopyIntoQuery(final int numRecords) {
        return copyIntoQuery.replaceFirst("COPY ", "COPY " + numRecords + " RECORDS ");
    }

    @Override
    public Integer execute() throws SQLException {
        final Connection conn;
        try {
            conn = getConnection();
        } catch (ClassNotFoundException e) {
            LOG.error(e);
            throw new SQLException(e.getMessage());
        }

        prepareTable(conn, createTableDDL, tableName);

        int numInserted = invokeCopyInto(conn, copyIntoQuery, tableName);
        return numInserted;
    }

    private static void prepareTable(Connection conn, String createTableDDL, String tableName)
            throws SQLException {
        try {
            JDBCUtils.update(conn, createTableDDL);
        } catch (SQLException e) {
            conn.rollback();
            if(LOG.isDebugEnabled()) {
                LOG.debug("Table already exists. Try to truncate " + tableName, e);
            }
            truncateTable(conn, tableName);
            // fall through
        }
    }

    private static int invokeCopyInto(Connection conn, String copyIntoQuery, String tableName)
            throws SQLException {
        final File loadFile = prepareLoadFile(tableName);
        final String query = complementCopyIntoQuery(copyIntoQuery, loadFile);
        final int ret;
        try {
            ret = JDBCUtils.update(conn, query);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction", e);
            conn.rollback();
            throw e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.debug(e);
            }
            if(!loadFile.delete()) {
                LOG.warn("Could not remove a tempolary file: " + loadFile.getAbsolutePath());
            }
        }
        return ret;
    }

    private static void truncateTable(@Nonnull final Connection conn, @Nonnull final String tableName)
            throws SQLException {
        String dml = "DELETE FROM " + tableName;
        JDBCUtils.update(conn, dml);
    }

    private static File prepareLoadFile(final String tableName) {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new IllegalStateException("Database directory not found: "
                    + colDir.getAbsoluteFile());
        }
        final File file = new File(colDir, tableName + ".csv");
        if(!file.exists()) {
            throw new IllegalStateException("Loading file not found: " + file.getAbsolutePath());
        }
        return file;
    }

    private static String complementCopyIntoQuery(final String query, final File loadFile) {
        String path = loadFile.getAbsolutePath();
        return query.replaceFirst("<src>", path);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.tableName = IOUtils.readString(in);
        this.createTableDDL = IOUtils.readString(in);
        this.copyIntoQuery = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(tableName, out);
        IOUtils.writeString(createTableDDL, out);
        IOUtils.writeString(copyIntoQuery, out);
    }

}

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
import gridool.db.record.DBRecord;
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV> <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DBInsertOperation extends DBOperation {
    private static final long serialVersionUID = -6835320411487501293L;
    private static final Log LOG = LogFactory.getLog(DBInsertOperation.class);

    @Nullable
    private/* final */String createTableDDL;
    @Nonnull
    private/* final */String tableName;
    @Nullable
    private/* final */String[] fieldNames;
    @Nonnull
    private/* final */DBRecord[] records;

    public DBInsertOperation() {
        super();
    } // for Externalizable

    public DBInsertOperation(String driverClassName, String connectUrl, @CheckForNull String tableName, @CheckForNull String[] fieldNames, @CheckForNull DBRecord[] records) {
        this(driverClassName, connectUrl, null, tableName, fieldNames, records);
    }

    public DBInsertOperation(String driverClassName, String connectUrl, @Nullable String createTableDDL, @CheckForNull String tableName, @CheckForNull String[] fieldNames, @CheckForNull DBRecord[] records) {
        super(driverClassName, connectUrl);
        checkParameters(tableName, fieldNames, records);
        this.createTableDDL = createTableDDL;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.records = records;
    }

    private static void checkParameters(String tableName, String[] fieldNames, DBRecord[] records) {
        if(tableName == null) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        if(records == null || records.length == 0) {
            throw new IllegalArgumentException("No insert record is not allowed");
        }
    }

    public String getCreateTableDDL() {
        return createTableDDL;
    }

    public String getTableName() {
        return tableName;
    }

    @SuppressWarnings("unchecked")
    public <T extends DBRecord> T[] getRecords() {
        return (T[]) records;
    }

    public Serializable execute() throws SQLException, GridException {
        final Connection conn = getConnection();
        if(createTableDDL != null) {
            try {
                executeDDL(conn, createTableDDL);
            } catch (SQLException e) {
                conn.rollback();
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Table already exists. Try to truncate " + tableName, e);
                }
                truncateTable(conn, tableName);
                // fall through
            }
        }
        final String insertSql = constructQuery(tableName, fieldNames, records);
        try {
            executeInsertQuery(conn, insertSql, records);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction", e);
            conn.rollback();
            throw e;
        } finally {
            conn.close();
        }
        return Boolean.TRUE;
    }

    private static void executeDDL(@Nonnull final Connection conn, @Nonnull final String sql)
            throws SQLException {
        final Statement st = conn.createStatement();
        try {
            st.executeUpdate(sql);
            conn.commit();
        } finally {
            st.close();
        }
    }

    private static void truncateTable(@Nonnull final Connection conn, @Nonnull final String table)
            throws SQLException {
        final Statement st = conn.createStatement();
        try {
            st.executeUpdate("DELETE FROM " + table);
            conn.commit();
        } finally {
            st.close();
        }
    }

    private static void executeInsertQuery(@Nonnull final Connection conn, @Nonnull final String sql, @Nonnull final DBRecord[] records)
            throws SQLException {
        final PreparedStatement stmt = conn.prepareStatement(sql);
        try {
            for(final DBRecord rec : records) {
                rec.writeFields(stmt);
                stmt.addBatch();
            }
            stmt.executeBatch();
        } finally {
            stmt.close();
        }
    }

    private static String constructQuery(@Nonnull final String tableName, @Nullable final String[] fieldNames, @Nonnull final DBRecord[] records) {
        final StringBuilder query = new StringBuilder(256);
        query.append("INSERT INTO ").append(tableName);
        final int numFields = (fieldNames == null) ? records[0].getNumFields() : fieldNames.length;
        final int last = numFields - 1;
        if(fieldNames != null && numFields > 0) {
            query.append(" (");
            for(int i = 0; i < numFields; i++) {
                query.append(fieldNames[i]);
                if(i != last) {
                    query.append(',');
                }
            }
            query.append(')');
        }
        query.append(" VALUES (");
        for(int i = 0; i < numFields; i++) {
            query.append('?');
            if(i != last) {
                query.append(',');
            }
        }
        query.append(");");
        return query.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.createTableDDL = IOUtils.readString(in);
        this.tableName = IOUtils.readString(in);
        final int numFields = in.readInt();
        final String[] fn;
        if(numFields == 0) {
            fn = null;
        } else {
            fn = new String[numFields];
            for(int i = 0; i < numFields; i++) {
                fn[i] = IOUtils.readString(in);
            }
        }
        this.fieldNames = fn;
        final int numRecords = in.readInt();
        final DBRecord[] r = new DBRecord[numRecords];
        for(int i = 0; i < numRecords; i++) {
            r[i] = (DBRecord) in.readObject();
        }
        this.records = r;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(createTableDDL, out);
        IOUtils.writeString(tableName, out);
        final String[] fn = fieldNames;
        final int numFields = (fn == null) ? 0 : fn.length;
        out.writeInt(numFields);
        for(int i = 0; i < numFields; i++) {
            IOUtils.writeString(fn[i], out);
        }
        final DBRecord[] r = records;
        final int numRecords = r.length;
        out.writeInt(numRecords);
        for(int i = 0; i < numRecords; i++) {
            out.writeObject(r[i]);
        }
    }

    public DBInsertOperation makeOperation(@Nonnull final DBRecord[] shrinkedRecords) {
        final DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, createTableDDL, tableName, fieldNames, shrinkedRecords);
        ops.userName = userName;
        ops.password = password;
        return ops;
    }

}
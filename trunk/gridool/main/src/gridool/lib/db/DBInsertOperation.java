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
package gridool.lib.db;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBInsertOperation extends DBOperation {
    private static final long serialVersionUID = -6835320411487501293L;
    private static final Log LOG = LogFactory.getLog(DBInsertOperation.class);

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
        super(driverClassName, connectUrl);
        checkParameters(tableName, fieldNames, records);
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.records = records;
    }

    private static void checkParameters(String tableName, String[] fieldNames, DBRecord[] records) {
        if(tableName == null) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        if(records == null) {
            throw new IllegalArgumentException("No insert record is not allowed");
        }
    }

    @Nonnull
    public String getTableName() {
        return tableName;
    }

    @SuppressWarnings("unchecked")
    public <T extends DBRecord> T[] getRecords() {
        return (T[]) records;
    }

    public Serializable execute() throws SQLException {
        final Connection conn;
        try {
            conn = getConnection();
        } catch (ClassNotFoundException e) {
            LOG.error(e);
            throw new SQLException(e.getMessage());
        }
        final String sql = constructQuery(tableName, fieldNames);
        try {
            executeQuery(conn, sql, records);
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

    private static void executeQuery(final Connection conn, final String sql, final DBRecord[] records)
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

    private static String constructQuery(@Nonnull final String tableName, final String[] fieldNames) {
        final StringBuilder query = new StringBuilder(256);
        query.append("INSERT INTO ").append(tableName);
        final int numFields = fieldNames.length;
        final int last = numFields - 1;
        if(numFields > 0) {
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
        this.tableName = IOUtils.readString(in);
        final int numFields = in.readInt();
        final String[] fn = new String[numFields];
        for(int i = 0; i < numFields; i++) {
            fn[i] = IOUtils.readString(in);
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
        return new DBInsertOperation(tableName, tableName, tableName, fieldNames, shrinkedRecords);
    }

}
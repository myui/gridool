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
package gridool.lib.db.monetdb;

import gridool.lib.db.DBOperation;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBCopyIntoOperation extends DBOperation implements Serializable {
    private static final long serialVersionUID = 1823389045268725519L;
    private static final Log LOG = LogFactory.getLog(MonetDBCopyIntoOperation.class);

    @Nullable
    private/* final */String createTableDDL;
    @Nonnull
    private/* final */String tableName;
    @Nonnull
    private/* final */String copyIntoQuery;
    @Nonnull
    private/* final */byte[] rowsData;

    private boolean compressed = false;

    public MonetDBCopyIntoOperation() {}

    public MonetDBCopyIntoOperation(String driverClassName, String connectUrl, @Nullable String createTableDDL, @Nonnull String tableName, @Nonnull String copyIntoQuery, @Nonnull byte[] rows) {
        super(driverClassName, connectUrl);
        this.createTableDDL = createTableDDL;
        this.tableName = tableName;
        this.copyIntoQuery = copyIntoQuery;
        this.rowsData = rows;
    }

    @Override
    public Serializable execute() throws SQLException {
        final Connection conn;
        try {
            conn = getConnection();
        } catch (ClassNotFoundException e) {
            LOG.error(e);
            throw new SQLException(e.getMessage());
        }
        if(createTableDDL != null) {// prepare a table
            try {
                executeUpdate(conn, createTableDDL);
            } catch (SQLException e) {
                conn.rollback();
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Table already exists. Try to truncate " + tableName, e);
                }
                truncateTable(conn, tableName);
                // fall through
            }
        }

        final File loadFile = prepareLoadFile(tableName, rowsData, compressed);
        this.rowsData = null;
        final String query = complementCopyIntoQuery(copyIntoQuery, loadFile);
        try {
            executeUpdate(conn, query);
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

        return Boolean.TRUE;
    }

    private static void executeUpdate(@Nonnull final Connection conn, @Nonnull final String sql)
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

    private static File prepareLoadFile(final String tableName, final byte[] data, final boolean compressed) {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new IllegalStateException("Database directory not found: "
                    + colDir.getAbsoluteFile());
        }
        final File loadFile;
        final FileOutputStream fos;
        try {
            loadFile = File.createTempFile(tableName, (compressed ? ".csv.gz" : ".csv"), colDir);
            fos = new FileOutputStream(loadFile);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create a load file", e);
        }
        try {
            BufferedOutputStream bos = new BufferedOutputStream(fos, 16384);
            bos.write(data, 0, data.length);
            bos.flush();
            bos.close();
        } catch (IOException e) {
            try {
                fos.close();
            } catch (IOException ioe) {
                LOG.debug(ioe);
            }
            throw new IllegalStateException("Failed to write data into file: "
                    + loadFile.getAbsolutePath(), e);
        }
        return loadFile;
    }

    private static String complementCopyIntoQuery(final String query, final File loadFile) {
        String path = loadFile.getAbsolutePath();
        return query.replaceFirst("<src>", path);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.createTableDDL = IOUtils.readString(in);
        this.tableName = IOUtils.readString(in);
        this.copyIntoQuery = IOUtils.readString(in);
        this.compressed = in.readBoolean();
        this.rowsData = IOUtils.readBytes(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(createTableDDL, out);
        IOUtils.writeString(tableName, out);
        IOUtils.writeString(copyIntoQuery, out);
        if(out instanceof OutputStream) {
            out.writeBoolean(true);
            OutputStream os = (OutputStream) out;
            GZIPOutputStream gos = new GZIPOutputStream(os, 8192);
            gos.write(rowsData);
            gos.finish();
            this.rowsData = null; // help GC
        } else {
            out.writeBoolean(false);
            IOUtils.writeBytes(rowsData, out);
        }
    }

}

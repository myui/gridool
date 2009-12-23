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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.util.io.FastBufferedOutputStream;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBPrepareCopyIntoOperation extends DBOperation implements Serializable {
    private static final long serialVersionUID = 1823389045268725519L;
    private static final Log LOG = LogFactory.getLog(MonetDBPrepareCopyIntoOperation.class);

    @Nullable
    private/* final */String createTableDDL;
    @Nonnull
    private/* final */String tableName;
    @Nonnull
    private/* final */byte[] rowsData;

    public MonetDBPrepareCopyIntoOperation() {}

    public MonetDBPrepareCopyIntoOperation(String driverClassName, String connectUrl, @Nullable String createTableDDL, @Nonnull String tableName, @Nonnull byte[] rows) {
        super(driverClassName, connectUrl);
        this.createTableDDL = createTableDDL;
        this.tableName = tableName;
        this.rowsData = rows;
    }

    @Override
    public Serializable execute() throws SQLException {
        final boolean firstTry = (createTableDDL != null);
        if(firstTry) {// prepare a table
            final Connection conn;
            try {
                conn = getConnection();
            } catch (ClassNotFoundException e) {
                LOG.error(e);
                throw new SQLException(e.getMessage());
            }
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

        prepareLoadFile(tableName, rowsData, !firstTry);
        this.rowsData = null;

        return Boolean.TRUE;
    }

    private static void truncateTable(@Nonnull final Connection conn, @Nonnull final String table)
            throws SQLException {
        String dml = "DELETE FROM " + table;
        JDBCUtils.update(conn, dml);
    }

    private static File prepareLoadFile(final String tableName, final byte[] data, final boolean append) {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new IllegalStateException("Database directory not found: "
                    + colDir.getAbsoluteFile());
        }
        final File loadFile;
        final FileOutputStream fos;
        try {
            loadFile = new File(colDir, tableName + ".csv");
            fos = new FileOutputStream(loadFile, append);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create a load file", e);
        }
        try {
            FastBufferedOutputStream bos = new FastBufferedOutputStream(fos, 8192);
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

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.createTableDDL = IOUtils.readString(in);
        this.tableName = IOUtils.readString(in);
        this.rowsData = IOUtils.readBytes(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(createTableDDL, out);
        IOUtils.writeString(tableName, out);
        IOUtils.writeBytes(rowsData, out);
    }

}

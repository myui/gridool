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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;

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
public final class DBExecuteUpdateOperation extends DBOperation {
    private static final long serialVersionUID = 7996807762151927537L;
    private static final Log LOG = LogFactory.getLog(DBExecuteUpdateOperation.class);

    @Nonnull
    private /* final */ String sql;
    
    public DBExecuteUpdateOperation() {}// for Externalizable

    public DBExecuteUpdateOperation(@Nonnull String driverClassName, @Nonnull String connectUrl, @Nonnull String sql) {
        super(driverClassName, connectUrl);
        this.sql = sql;
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
        return executeUpdate(conn, sql);
    }

    private static int executeUpdate(@Nonnull final Connection conn, @Nonnull final String sql)
            throws SQLException {
        final Statement st = conn.createStatement();
        final int ret;
        try {
            ret = st.executeUpdate(sql);
            conn.commit();
        } finally {
            st.close();
        }
        return ret;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.sql = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(sql, out);
    }

}

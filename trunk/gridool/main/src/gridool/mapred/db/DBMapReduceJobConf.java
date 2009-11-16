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
package gridool.mapred.db;

import gridool.mapred.DataSource;
import gridool.mapred.db.task.DBMapShuffleTask;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBMapReduceJobConf implements Serializable {
    private static final long serialVersionUID = -5171035363060721859L;

    protected String mapOutputTableName = null;
    protected String reduceOutputTableName = null;

    public DBMapReduceJobConf() {}

    public final Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(getDriverClassName());
        final String url = getConnectUrl();
        final String user = getUserName();
        final Connection conn;
        if(user == null) {
            conn = DriverManager.getConnection(url);
        } else {
            String password = getPassword();
            conn = DriverManager.getConnection(url, user, password);
        }
        configure(conn);
        return conn;
    }

    public abstract String getDriverClassName();

    public abstract String getConnectUrl();

    public String getUserName() {
        return null;
    }

    public String getPassword() {
        return null;
    }

    protected void configure(@Nonnull final Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        conn.setReadOnly(true);
    }

    public abstract String getInputQuery();

    public abstract DBRecord createInputRecord();

    public abstract DataSource getDataSinkForShuffleOutput();

    public final String getMapOutputTableName() {
        return mapOutputTableName;
    }
    
    public final void setMapOutputTableName(@Nonnull String mapOutputTableName) {
        this.mapOutputTableName = mapOutputTableName;
    }
    
    @Nullable
    public String[] getMapOutputFieldNames() {
        return null;
    }

    public final String getReduceOutputTableName() {
        return reduceOutputTableName;
    }

    public final void setReduceOutputTableName(@Nonnull String reduceOutputTableName) {
        this.reduceOutputTableName = reduceOutputTableName;
    }

    public abstract DBMapShuffleTask makeMapShuffleTask(@Nonnull DBMapJob dbMapJob, @Nonnull String destTableName);

}

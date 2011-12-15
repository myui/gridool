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
package gridool.db.partitioning.phihash;

import gridool.GridTask;

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
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DBPartitioningJobConf implements Serializable {
    private static final long serialVersionUID = 636573640789390674L;

    public DBPartitioningJobConf() {}

    @Nonnull
    public abstract PartitioningJobType getJobType();

    // -------------------------------------------------------------
    // CSV stuffs

    @Nonnull
    public abstract String getCsvFilePath();

    @Nonnull
    public abstract String getTableName();

    @Nonnull
    public abstract String getBaseTableName();

    public char getFieldSeparator() {
        return '\t';
    }

    public String getRecordSeparator() {
        return "\n";
    }

    public char getStringQuote() {
        return '\"';
    }

    public abstract int getNumberOfBuckets();

    // -------------------------------------------------------------
    // JDBC stuffs

    @Nonnull
    public abstract String getDriverClassName();

    @Nonnull
    public abstract String getConnectUrl();

    @Nullable
    public String getUserName() {
        return null;
    }

    @Nullable
    public String getPassword() {
        return null;
    }

    @Nonnull
    public abstract String getCreateTableDDL();

    @Nullable
    public String getAlterTableDDL() {
        return null;
    }

    @Nonnull
    public final Connection getConnection(boolean autoCommit) throws ClassNotFoundException,
            SQLException {
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
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    @Nonnull
    public abstract GridTask makePartitioningTask(DBPartitioningJob job);

}

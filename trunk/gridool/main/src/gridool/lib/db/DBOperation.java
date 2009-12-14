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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBOperation implements Externalizable {
    private static final long serialVersionUID = -4228703722773423150L;

    @Nonnull
    protected/* final */String driverClassName;
    @Nonnull
    protected/* final */String connectUrl;
    @Nullable
    protected String userName = null;
    @Nullable
    protected String password = null;

    public DBOperation() {}// Externalizable

    public DBOperation(@CheckForNull String driverClassName, @CheckForNull String connectUrl) {
        if(driverClassName == null) {
            throw new IllegalArgumentException("Driver class must be specified");
        }
        if(connectUrl == null) {
            throw new IllegalArgumentException("Connect Url must be specified");
        }
        this.driverClassName = driverClassName;
        this.connectUrl = connectUrl;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getConnectUrl() {
        return connectUrl;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public final void setAuth(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public final Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(driverClassName);
        final Connection conn;
        if(userName == null) {
            conn = DriverManager.getConnection(connectUrl);
        } else {
            conn = DriverManager.getConnection(connectUrl, userName, password);
        }
        configureConnection(conn);
        return conn;
    }

    protected void configureConnection(@Nonnull Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public abstract Serializable execute() throws SQLException;

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(driverClassName, out);
        IOUtils.writeString(connectUrl, out);
        IOUtils.writeString(userName, out);
        IOUtils.writeString(password, out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.driverClassName = IOUtils.readString(in);
        this.connectUrl = IOUtils.readString(in);
        this.userName = IOUtils.readString(in);
        this.password = IOUtils.readString(in);
    }
}

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
package gridool.replication.jobs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
public final class CoordinateReplicaJobConf implements Externalizable {

    @Nonnull
    private String driverClassName;
    @Nonnull
    private String primaryDbUrl;
    @Nullable
    private String user;
    @Nullable
    private String passwd;

    private int numReplicas;
    private boolean reorg;

    public CoordinateReplicaJobConf() {}// for Externalizable

    public CoordinateReplicaJobConf(String driverClassName, String primaryDbUrl, String user, String passwd, int numReplicas, boolean reorg) {
        if(driverClassName == null) {
            throw new IllegalArgumentException();
        }
        if(primaryDbUrl == null) {
            throw new IllegalArgumentException();
        }
        this.driverClassName = driverClassName;
        this.primaryDbUrl = primaryDbUrl;
        this.user = user;
        this.passwd = passwd;
        this.numReplicas = numReplicas;
        this.reorg = reorg;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getPrimaryDbUrl() {
        return primaryDbUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPasswd() {
        return passwd;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public boolean isReorg() {
        return reorg;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.driverClassName = IOUtils.readString(in);
        this.primaryDbUrl = IOUtils.readString(in);
        this.user = IOUtils.readString(in);
        this.passwd = IOUtils.readString(in);
        this.numReplicas = in.readInt();
        this.reorg = in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(driverClassName, out);
        IOUtils.writeString(primaryDbUrl, out);
        IOUtils.writeString(user, out);
        IOUtils.writeString(passwd, out);
        out.writeInt(numReplicas);
        out.writeBoolean(reorg);
    }
}
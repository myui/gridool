/*
 * @(#)$Id$
 *
 * Copyright 2006-2010 Makoto YUI
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

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.IdentityHashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en">
 * returns the minimum # of created replicas 
 * </DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class CoordinateReplicaJob extends GridJobBase<CoordinateReplicaJob.JobConf, Integer> {
    private static final long serialVersionUID = 7562696673301106475L;

    private transient int minReplicas = -1;

    public CoordinateReplicaJob() {
        super();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, CoordinateReplicaJob.JobConf jobConf)
            throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> mapping = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GridTask task = new CoordinateReplicaTask(this, jobConf);
            mapping.put(task, node);
        }
        return mapping;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        Integer numReplicaCreated = result.getResult();
        if(numReplicaCreated != null) {
            if(minReplicas == -1) {
                minReplicas = numReplicaCreated.intValue();
            } else {
                minReplicas = Math.min(minReplicas, numReplicaCreated.intValue());
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Integer reduce() throws GridException {
        return minReplicas;
    }

    public static final class JobConf implements Externalizable {

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

        public JobConf() {}// for Externalizable

        public JobConf(String driverClassName, String primaryDbUrl, String user, String passwd, int numReplicas, boolean reorg) {
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

        String getDriverClassName() {
            return driverClassName;
        }

        String getPrimaryDbUrl() {
            return primaryDbUrl;
        }

        String getUser() {
            return user;
        }

        String getPasswd() {
            return passwd;
        }

        int getNumReplicas() {
            return numReplicas;
        }

        boolean isReorg() {
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

}

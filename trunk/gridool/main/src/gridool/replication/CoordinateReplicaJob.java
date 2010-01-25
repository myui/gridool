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
package gridool.replication;

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

    private int minReplicas = -1;

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

        private int numReplicas;
        private boolean reorg;

        public JobConf(int numReplicas, boolean reorg) {
            this.numReplicas = numReplicas;
            this.reorg = reorg;
        }

        int getNumReplicas() {
            return numReplicas;
        }

        boolean isReorg() {
            return reorg;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.numReplicas = in.readInt();
            this.reorg = in.readBoolean();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(numReplicas);
            out.writeBoolean(reorg);
        }

    }

}

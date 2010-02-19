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
package gridool.db.catalog;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.io.IOUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class RegisterPatitionInCatalogJob extends
        GridJobBase<RegisterPatitionInCatalogJob.JobConf, Boolean> {
    private static final long serialVersionUID = -4173733362983065008L;
    private static final Log LOG = LogFactory.getLog(RegisterPatitionInCatalogJob.class);

    public RegisterPatitionInCatalogJob() {
        super();
    }

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final JobConf jobConf)
            throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(final GridNode node : nodes) {
            GridTask task = new RegisterPartitionInCatalogTask(this, jobConf);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Boolean succeed = result.getResult();
        if(succeed == null) {
            if(LOG.isWarnEnabled()) {
                GridNode node = result.getExecutedNode();
                GridException err = result.getException();
                LOG.warn("UpdateCatalogTask failed on node: " + node, err);
            }
        } else {
            assert (succeed.booleanValue());
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return Boolean.TRUE; // TODO should return failed nodes?
    }

    private static final class RegisterPartitionInCatalogTask extends GridTaskAdapter {
        private static final long serialVersionUID = -7634435239684206969L;

        private final JobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected RegisterPartitionInCatalogTask(GridJob job, JobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Boolean execute() throws GridException {
            final String distKey = jobConf.distributionKey;
            final DistributionCatalog catalog = registry.getDistributionCatalog();
            final List<Pair<GridNode, List<GridNode>>> masterSlaves = jobConf.masterSlaves;
            assert (masterSlaves != null);
            for(Pair<GridNode, List<GridNode>> e : masterSlaves) {
                GridNode master = e.getFirst();
                List<GridNode> slaves = e.getSecond();
                catalog.registerPartition(master, slaves, distKey);
            }
            return Boolean.TRUE;
        }

    }

    public static final class JobConf implements Externalizable {

        @Nonnull
        private String distributionKey;
        @Nonnull
        private List<Pair<GridNode, List<GridNode>>> masterSlaves;

        public JobConf() {}// for Externalizable

        public JobConf(@Nonnull String distributionKey, @Nonnull List<Pair<GridNode, List<GridNode>>> masterSlaves) {
            this.distributionKey = distributionKey;
            this.masterSlaves = masterSlaves;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.distributionKey = IOUtils.readString(in);
            int size = in.readInt();
            final List<Pair<GridNode, List<GridNode>>> masterSlaves = new ArrayList<Pair<GridNode, List<GridNode>>>(size);
            for(int i = 0; i < size; i++) {
                GridNode master = (GridNode) in.readObject();
                final int numSlaves = in.readInt();
                final List<GridNode> slaves = new ArrayList<GridNode>(numSlaves);
                for(int j = 0; j < numSlaves; j++) {
                    GridNode node = (GridNode) in.readObject();
                    slaves.add(node);
                }
                masterSlaves.add(new Pair<GridNode, List<GridNode>>(master, slaves));
            }
            this.masterSlaves = masterSlaves;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(distributionKey, out);
            int size = masterSlaves.size();
            out.writeInt(size);
            for(Pair<GridNode, List<GridNode>> e : masterSlaves) {
                GridNode master = e.getFirst();
                List<GridNode> slaves = e.getSecond();
                out.writeObject(master);
                int numSlaves = slaves.size();
                out.writeInt(numSlaves);
                for(GridNode slave : slaves) {
                    out.writeObject(slave);
                }
            }
        }

    }

}

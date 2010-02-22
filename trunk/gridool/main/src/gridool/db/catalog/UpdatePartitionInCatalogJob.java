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

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

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
public final class UpdatePartitionInCatalogJob extends
        GridJobBase<UpdatePartitionInCatalogJob.UpdatePartitionInCatalogJobConf, Boolean> {
    private static final long serialVersionUID = 6798774505368347283L;
    private static final Log LOG = LogFactory.getLog(UpdatePartitionInCatalogJob.class);

    @GridConfigResource
    private transient GridConfiguration config;

    public UpdatePartitionInCatalogJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, UpdatePartitionInCatalogJobConf jobConf)
            throws GridException {
        final GridNode localNode = config.getLocalNode();
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(final GridNode node : nodes) {
            if(!node.equals(localNode)) {
                GridTask task = new UpdatePartitionInCatalogTask(this, jobConf);
                map.put(task, node);
            }
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
        return Boolean.TRUE;
    }

    private static final class UpdatePartitionInCatalogTask extends GridTaskAdapter {
        private static final long serialVersionUID = -4236772054123268197L;

        private final UpdatePartitionInCatalogJobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registery;

        @SuppressWarnings("unchecked")
        UpdatePartitionInCatalogTask(GridJob job, UpdatePartitionInCatalogJobConf jobConf) {
            super(job, false);
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Boolean execute() throws GridException {
            DistributionCatalog catalog = registery.getDistributionCatalog();
            catalog.updatePartitioningInformation(jobConf.getTableName(), jobConf.getFieldPartitionMap());
            return Boolean.TRUE;
        }

    }

    public static final class UpdatePartitionInCatalogJobConf implements Externalizable {

        @Nonnull
        private/* final */String tableName;
        @Nonnull
        private/* final */Map<String, PartitionKey> fieldPartitionMap;

        public UpdatePartitionInCatalogJobConf() {} // for Externalizable

        public UpdatePartitionInCatalogJobConf(@Nonnull String tableName, @Nonnull Map<String, PartitionKey> fieldPartitionMap) {
            this.tableName = tableName;
            this.fieldPartitionMap = fieldPartitionMap;
        }

        public String getTableName() {
            return tableName;
        }

        public Map<String, PartitionKey> getFieldPartitionMap() {
            return fieldPartitionMap;
        }

        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            this.tableName = IOUtils.readString(in);
            final int size = in.readInt();
            final Map<String, PartitionKey> map = new HashMap<String, PartitionKey>(size); // should not be a IdentityHashMap
            for(int i = 0; i < size; i++) {
                String columnName = IOUtils.readString(in);
                PartitionKey key = (PartitionKey) in.readObject();
                map.put(columnName, key);
            }
            this.fieldPartitionMap = map;
        }

        public void writeExternal(final ObjectOutput out) throws IOException {
            IOUtils.writeString(tableName, out);
            final int size = fieldPartitionMap.size();
            out.writeInt(size);
            for(final Map.Entry<String, PartitionKey> e : fieldPartitionMap.entrySet()) {
                String columnName = e.getKey();
                IOUtils.writeString(columnName, out);
                PartitionKey partkey = e.getValue();
                out.writeObject(partkey);
            }
        }

    }
}

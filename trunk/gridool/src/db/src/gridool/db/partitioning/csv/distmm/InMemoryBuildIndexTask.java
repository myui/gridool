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
package gridool.db.partitioning.csv.distmm;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.NodeWithPartitionNo;
import gridool.db.partitioning.csv.distmm.InMemoryLocalCsvHashPartitioningJob.DerivedFragmentInfo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class InMemoryBuildIndexTask extends GridTaskAdapter {
    private static final long serialVersionUID = 7085923588933253600L;

    private transient/* final */List<DerivedFragmentInfo> storeList;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public InMemoryBuildIndexTask(@Nonnull GridJob job, @Nonnull List<DerivedFragmentInfo> storeList) {
        super(job, false);
        this.storeList = storeList;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected Boolean execute() throws GridException {
        // build index for derived fragments
        final InMemoryMappingIndex index = registry.getMappingIndex();
        for(DerivedFragmentInfo info : storeList) {
            String idxName = info.getFkIdxName();
            String key = info.getDistkey();
            NodeWithPartitionNo np = info.getValue();
            index.addEntry(idxName, key, np);
        }
        this.storeList = null; // help GC
        return Boolean.TRUE;
    }

    private void readObject(ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();

        final int size = s.readInt();
        final DerivedFragmentInfo[] infos = new DerivedFragmentInfo[size];
        for(int i = 0; i < size; i++) {
            infos[i] = DerivedFragmentInfo.readFrom(s);
        }
        this.storeList = Arrays.asList(infos);
    }

    private void writeObject(ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();

        final List<DerivedFragmentInfo> list = storeList;
        final int size = list.size();
        s.writeInt(size);
        for(int i = 0; i < size; i++) {
            DerivedFragmentInfo info = list.get(i);
            info.writeExternal(s);
        }
    }
}

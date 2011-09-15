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
package gridool.db.partitioning.phihash.csv.dist;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.phihash.csv.dist.LocalCsvHashPartitioningJob.DerivedFragmentInfo;
import gridool.dht.ILocalDirectory;
import gridool.dht.btree.IndexException;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridBuildIndexTask extends GridTaskAdapter {
    private static final long serialVersionUID = 7085923588933253600L;

    private transient/* final */List<DerivedFragmentInfo> storeList;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public GridBuildIndexTask(@Nonnull GridJob job, @Nonnull List<DerivedFragmentInfo> storeList) {
        super(job, false);
        this.storeList = storeList;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected Boolean execute() throws GridException {
        final Map<String, List<DerivedFragmentInfo>> map = new HashMap<String, List<DerivedFragmentInfo>>(12);
        for(final DerivedFragmentInfo e : storeList) {
            String idxName = e.getFkIdxName();
            List<DerivedFragmentInfo> storeList = map.get(idxName);
            if(storeList == null) {
                storeList = new ArrayList<DerivedFragmentInfo>(1024);
                map.put(idxName, storeList);
            }
            storeList.add(e);
        }
        // build index for derived fragments
        final ILocalDirectory index = registry.getDirectory();
        for(final Map.Entry<String, List<DerivedFragmentInfo>> e : map.entrySet()) {
            List<DerivedFragmentInfo> list = e.getValue();
            Collections.sort(list);
            int size = list.size();
            final byte[][] keys = new byte[size][];
            final byte[][] values = new byte[size][];
            for(int i = 0; i < size; i++) {
                DerivedFragmentInfo info = list.get(i);
                keys[i] = info.getDistkey();
                values[i] = info.getValue();
            }
            final String idxName = e.getKey();
            synchronized(index) {// sync for batch update
                try {
                    index.addMapping(idxName, keys, values);
                } catch (IndexException dbe) {
                    throw new GridException("failed to build an index: " + idxName, dbe);
                }
            }
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

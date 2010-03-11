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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.csv.ParallelCsvHashPartitioningJob.DerivedFragmentInfo;
import gridool.directory.ILocalDirectory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import xbird.storage.DbException;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridIndexBuildTask extends GridTaskAdapter {
    private static final long serialVersionUID = 7085923588933253600L;

    private final List<DerivedFragmentInfo> storeList;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public GridIndexBuildTask(@Nonnull GridJob job, @Nonnull List<DerivedFragmentInfo> storeList) {
        super(job, false);
        this.storeList = storeList;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected Boolean execute() throws GridException {
        final ILocalDirectory index = registry.getDirectory();

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
        for(final Map.Entry<String, List<DerivedFragmentInfo>> e : map.entrySet()) {
            List<DerivedFragmentInfo> list = e.getValue();
            int size = list.size();
            final byte[][] keys = new byte[size][];
            final byte[][] values = new byte[size][];
            for(int i = 0; i < size; i++) {
                DerivedFragmentInfo info = list.get(i);
                keys[i] = info.getDistkey();
                values[i] = info.getValue();
            }
            final String idxName = e.getKey();
            try {
                index.addMapping(idxName, keys, values);
            } catch (DbException dbe) {
                throw new GridException("failed to build an index: " + idxName, dbe);
            }
        }
        return Boolean.TRUE;
    }

}

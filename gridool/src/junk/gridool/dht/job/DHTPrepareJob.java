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
package gridool.dht.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.dht.construct.DHTEntrySnapshot;
import gridool.dht.construct.DHTSnapshot;
import gridool.dht.construct.ValueWithVersion;
import gridool.dht.ops.MultiKeyOperation;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTPrepareJob extends MultiKeyDHTJob<DHTSnapshot> {
    private static final long serialVersionUID = -3999179074402080876L;

    private Map<String, ValueWithVersion> _resultMap = null;

    public DHTPrepareJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridTaskRouter router, MultiKeyOperation ops)
            throws GridException {
        final String[] keys = ops.getKeys();
        this._resultMap = new HashMap<String, ValueWithVersion>(keys.length);
        for(String key : keys) {
            _resultMap.put(key, null);
        }

        return super.map(router, ops);
    }

    @Override
    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        assert (_resultMap != null);

        final GridNode residentNode = result.getExecutedNode();
        final DHTSnapshot snapshot = result.getResult();

        if(snapshot != null) {
            final List<DHTEntrySnapshot> entries = snapshot.getSnapshotEntries();
            for(DHTEntrySnapshot entry : entries) {
                final String key = entry.getKey();

                long latestVersion = Long.MIN_VALUE;
                final ValueWithVersion prev = _resultMap.get(key);
                if(prev != null) {
                    latestVersion = prev.getVersion();
                }

                if(entry.isLockAcquired()) {
                    final ValueWithVersion[] ary = entry.getValues();
                    for(ValueWithVersion v : ary) {
                        final long version = v.getVersion();
                        if(version > latestVersion) {
                            v.setResidentNode(residentNode);
                            _resultMap.put(key, v);
                        }
                    }
                }
            }
        }

        return GridTaskResultPolicy.CONTINUE;
    }

    @Override
    public DHTSnapshot reduce() throws GridException {
        return null;
    }

}

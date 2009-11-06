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
package gridool.mapred.dht;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.mapred.dht.task.DhtReduceTask;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DhtReduceJob extends GridJobBase<DhtMapReduceJobConf, String> {
    private static final long serialVersionUID = 3291003882839698498L;

    private transient String destDhtName;

    public DhtReduceJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, DhtMapReduceJobConf logic)
            throws GridException {
        final String inputDhtName = logic.getInputDhtName();
        final String destDhtName = generateOutputDhtName(inputDhtName, System.nanoTime());
        this.destDhtName = destDhtName;

        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            DhtReduceTask task = logic.makeReduceTask(this, inputDhtName, destDhtName);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public String reduce() throws GridException {
        return destDhtName;
    }

    private static String generateOutputDhtName(final String inputDhtName, final long time) {
        final int endIndex = inputDhtName.indexOf("#output-");
        if(endIndex != -1) {
            String baseName = inputDhtName.substring(0, endIndex);
            return baseName + "#output-" + time;
        }
        return inputDhtName + "#output-" + time;
    }

}

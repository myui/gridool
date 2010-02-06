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
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DhtReduceJob extends GridJobBase<DhtMapReduceJobConf, String> {
    private static final long serialVersionUID = 3291003882839698498L;

    private transient String destTableName;

    public DhtReduceJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, DhtMapReduceJobConf jobConf)
            throws GridException {
        final String inputTableName = jobConf.getInputTableName();
        String destTableName = jobConf.getOutputTableName();
        if(destTableName == null) {
            destTableName = generateOutputTableName(inputTableName, System.nanoTime());
        }
        this.destTableName = destTableName;

        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GridTask task = jobConf.makeReduceTask(this, inputTableName, destTableName);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public String reduce() throws GridException {
        return destTableName;
    }

    private static String generateOutputTableName(final String inputTableName, final long time) {
        final int endIndex = inputTableName.indexOf("#output-");
        if(endIndex != -1) {
            String baseName = inputTableName.substring(0, endIndex);
            return baseName + "#output-" + time;
        }
        return inputTableName + "#output-" + time;
    }

}

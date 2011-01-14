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
import gridool.mapred.dht.task.DhtMapShuffleTask;
import gridool.routing.GridRouter;
import gridool.util.lang.ObjectUtils;

import java.util.IdentityHashMap;
import java.util.Map;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class DhtMapJob extends GridJobBase<byte[], String> {
    private static final long serialVersionUID = -1854038331598309550L;

    protected transient long jobStartTime;
    protected transient DhtMapReduceJobConf jobConf;
    protected transient String inputDhtName;
    protected transient String shuffleDestDhtName;

    public DhtMapJob() {
        super();
    }

    public final Map<GridTask, GridNode> map(GridRouter router, byte[] rawLogic)
            throws GridException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        DhtMapReduceJobConf logic = ObjectUtils.readObjectQuietly(rawLogic, cl);
        this.jobStartTime = System.nanoTime();
        this.jobConf = logic;
        String inputDhtName = logic.getInputTableName();
        this.inputDhtName = inputDhtName;
        String destDhtName = generateIntermediateDhtName(inputDhtName, jobStartTime);
        this.shuffleDestDhtName = destDhtName;

        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            DhtMapShuffleTask task = logic.makeMapShuffleTask(this, inputDhtName, destDhtName);
            map.put(task, node);
        }
        return map;
    }

    public final GridTaskResultPolicy result(GridTaskResult result)
            throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public String reduce() throws GridException {
        return shuffleDestDhtName;
    }

    private static String generateIntermediateDhtName(final String inputDhtName, final long time) {
        final int endIndex = inputDhtName.indexOf("#shuffle-");
        if(endIndex != -1) {
            String baseName = inputDhtName.substring(0, endIndex);
            return baseName + "#shuffle-" + time;
        }
        return inputDhtName + "#shuffle-" + time;
    }

}

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
package gridool.mapred.dht.task;

import gridool.GridJob;
import gridool.GridNode;
import gridool.directory.job.DirectoryDestinatedAddJob;
import gridool.directory.ops.DestinatedAddOperation;
import gridool.util.collections.BoundedArrayQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DhtGatherReduceTask extends DhtScatterReduceTask {
    private static final long serialVersionUID = 7961249443055449973L;

    @SuppressWarnings("unchecked")
    public DhtGatherReduceTask(GridJob job, String inputTblName, String destTblName, boolean removeInputDhtOnFinish) {
        super(job, inputTblName, destTblName, removeInputDhtOnFinish);
    }
    
    @Override
    protected final boolean collectOutputKeys() {
        return false;
    }

    @Override
    protected final void invokeShuffle(final BoundedArrayQueue<byte[]> queue) {
        assert (kernel != null);

        GridNode dstNode = getSenderNode();
        final DestinatedAddOperation ops = new DestinatedAddOperation(destTableName, dstNode);
        ops.setMaxNumReplicas(0); // TODO

        final int size = queue.size();
        for(int i = 0; i < size; i += 2) {
            byte[] k = queue.get(i);
            byte[] v = queue.get(i + 1);
            ops.addMapping(k, v);
        }

        shuffleExecPool.execute(new Runnable() {
            public void run() {
                kernel.execute(DirectoryDestinatedAddJob.class, ops);
            }
        });
    }

}

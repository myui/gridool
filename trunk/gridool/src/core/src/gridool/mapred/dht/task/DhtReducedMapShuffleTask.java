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
import gridool.dht.btree.Value;
import gridool.dht.helpers.FlushableBTreeCallback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DhtReducedMapShuffleTask extends DhtMapShuffleTask {
    private static final long serialVersionUID = -3603582178622867565L;

    @SuppressWarnings("unchecked")
    public DhtReducedMapShuffleTask(GridJob job, String inputTblName, String destTblName, boolean removeInputDhtOnFinish) {
        super(job, inputTblName, destTblName, removeInputDhtOnFinish);
    }

    @Override
    protected final ReducedMapHandler getHandler() {
        return new ReducedMapHandler(this);
    }

    @Override
    protected final boolean process(byte[] key, byte[] value) {
        throw new IllegalStateException();
    }

    /**
     * @return true/false to continue/stop mapping.
     */
    protected abstract boolean process(@Nonnull byte[] key, @Nonnull Collection<byte[]> values);

    private static final class ReducedMapHandler implements FlushableBTreeCallback {

        private final DhtReducedMapShuffleTask parent;

        private Value prevKey = null;
        private List<byte[]> values;

        private int counter = 0;

        public ReducedMapHandler(DhtReducedMapShuffleTask task) {
            super();
            this.parent = task;
            this.values = new ArrayList<byte[]>(4);
        }

        public boolean indexInfo(final Value key, final byte[] value) {
            boolean doNext = true;
            if(prevKey != null) {
                if(key == prevKey || key.equals(prevKey)) {
                    values.add(value);
                    return true;
                }
                // different key to the previous key was found.            
                // Then, flush previous entry
                doNext = parent.process(prevKey.getData(), values);
                if((++counter) == 10) {
                    parent.reportProgress(-1f);
                }
                this.values = new ArrayList<byte[]>(4);
            }
            this.prevKey = key;

            final byte[] keyData = key.getData();
            if(!parent.filter(keyData, value)) {
                values.add(value);
            }
            return doNext;
        }

        public boolean indexInfo(Value value, long pointer) {
            throw new IllegalStateException();
        }

        public void flush() {
            if(!values.isEmpty()) {
                parent.process(prevKey.getData(), values);
            }
        }
    }

}

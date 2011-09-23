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
package gridool.dht.mapred;

import gridool.GridJob;
import gridool.dht.mapred.task.DhtMapShuffleTask;
import gridool.dht.mapred.task.DhtReduceTask;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;
import xbird.util.lang.ObjectUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class JobConfBase extends MapReduceJobConf implements Externalizable {
    private static final long serialVersionUID = -8849418189501054900L;

    @Nullable
    private transient DhtMapShuffleTask mapTask;
    @Nullable
    private transient DhtReduceTask reduceTask;

    private transient byte[] rawMapTask;
    private transient byte[] rawReduceTask;

    public JobConfBase() {}// for Externalizable

    public JobConfBase(@Nonnull String inputDhtName) {
        super(inputDhtName);
    }

    public JobConfBase(@Nonnull String inputDhtName, @Nullable DhtMapShuffleTask map, @Nullable DhtReduceTask reduce) {
        super(inputDhtName);
        this.mapTask = map;
        this.reduceTask = reduce;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputDhtName, String destDhtName) {
        if(mapTask == null) {
            if(rawMapTask != null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                this.mapTask = ObjectUtils.readObjectQuietly(rawMapTask, cl);
                this.rawMapTask = null;
            }
        }
        return mapTask;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected DhtReduceTask makeReduceTask(GridJob job, String inputDhtName, String destDhtName) {
        if(reduceTask == null) {
            if(rawReduceTask != null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                this.reduceTask = ObjectUtils.readObjectQuietly(rawReduceTask, cl);
                this.rawReduceTask = null;
            }
        }
        return reduceTask;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] b;
        if(mapTask == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            b = ObjectUtils.toBytes(mapTask);
            IOUtils.writeBytes(b, out);
        }
        if(reduceTask == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            b = ObjectUtils.toBytes(reduceTask);
            IOUtils.writeBytes(b, out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean()) {
            this.rawMapTask = IOUtils.readBytes(in); // lazy instantiation
        }
        if(in.readBoolean()) {
            this.rawReduceTask = IOUtils.readBytes(in);
        }
    }

}

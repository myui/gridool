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
package gridool.db.partitioning.phihash.csv.pgrace;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.phihash.csv.distmm.InMemoryIndexHelper;
import gridool.db.partitioning.phihash.csv.distmm.InMemoryMappingIndex;
import gridool.locking.LockManager;
import gridool.util.io.FastByteArrayInputStream;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nonnull;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ParallelGraceBuildIndexTask extends GridTaskAdapter {
    private static final long serialVersionUID = 7085923588933253600L;

    private transient FastByteArrayOutputStream out;
    private transient/* final */byte[] value;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public ParallelGraceBuildIndexTask(@Nonnull GridJob job, @Nonnull FastByteArrayOutputStream out) {
        super(job, false);
        this.out = out;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected Boolean execute() throws GridException {
        LockManager lockmgr = registry.getLockManager();
        InMemoryMappingIndex mmidx = registry.getMappingIndex();
        ReadWriteLock lock = lockmgr.obtainLock(mmidx);
        Lock wlock = lock.writeLock();

        if(value == null) {//for local task
            assert (out != null);
            this.value = out.toByteArray();
            this.out = null;
        }

        final FastByteArrayInputStream in = new FastByteArrayInputStream(value);
        wlock.lock();
        try {
            InMemoryIndexHelper.writeToFile(in);
        } catch (IOException e) {
            throw new GridException(e);
        } finally {
            wlock.unlock();
        }
        this.value = null; // help GC
        return Boolean.TRUE;
    }

    private void readObject(ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        this.value = IOUtils.readBytes(s);
    }

    private void writeObject(ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
        int size = out.size();
        s.writeInt(size);
        out.writeTo(s);
        this.out = null;
        //IOUtils.writeBytes(value, s);
    }

}

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
package gridool.memcached;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.Nonnull;

import gridool.GridConfiguration;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.memcached.cache.MCElementSnapshot;
import gridool.memcached.construct.BooleanResultBroadcastJob;
import gridool.memcached.construct.BooleanResultJob;
import gridool.memcached.construct.CASResultJob;
import gridool.memcached.construct.GetMultiJob;
import gridool.memcached.construct.GetSnapshotResultJob;
import gridool.memcached.construct.MemcachedOperationFactory;
import gridool.memcached.construct.ObjectResultJob;
import gridool.memcached.ops.MemcachedOperation;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MemcachedClient implements Memcached {

    @Nonnull
    private final GridKernel grid;
    private final int maxReplicas;

    private int numberOfReplicas = 0;

    public MemcachedClient(GridKernel grid, GridConfiguration config) {
        if(grid == null) {
            throw new IllegalArgumentException();
        }
        this.grid = grid;
        this.maxReplicas = config.getNumberOfReplicas();
    }

    public void setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = Math.min(numberOfReplicas, maxReplicas);
    }

    @Override
    public void stop() {}

    // --------------------------------------

    @Override
    public MCElementSnapshot prepareOperation(String key, boolean writeOps) {
        MemcachedOperation ops = MemcachedOperationFactory.createPrepareOperation(key, writeOps);
        return doSyncJob(GetSnapshotResultJob.class, ops);
    }

    @Override
    public void abortOperation(String key, boolean writeOps) {

    }

    @Override
    public <T extends Serializable> void commitOperation(String key, T value, boolean writeOps) {

    }

    // --------------------------------------
    // Storage command

    @Override
    public <T extends Serializable> boolean set(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createSetOperation(key, value, false);
        return doBooleanSyncJob(ops, false);
    }

    public <T extends Serializable> void asyncSet(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createSetOperation(key, value, true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    @Override
    public <T extends Serializable> boolean add(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createAddOperation(key, value, false);
        return doBooleanSyncJob(ops, false);
    }

    public <T extends Serializable> void asyncAdd(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createAddOperation(key, value, true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    @Override
    public <T extends Serializable> Serializable replace(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createReplaceOperation(key, value);
        return doSyncJob(ObjectResultJob.class, ops);
    }

    // --------------------------------------
    // Additional storage command

    /**
     * A check and set operation which means "store this data but 
     * only if no one else has updated since I last fetched it."
     */
    @Override
    public <T extends Serializable> Serializable cas(String key, long casId, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createCASOperation(key, casId, value, false);
        return doSyncJob(ObjectResultJob.class, ops);
    }

    public <T extends Serializable> void asyncCAS(String key, long casId, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createCASOperation(key, casId, value, true);
        doAsyncJob(ObjectResultJob.class, ops);
    }

    @Override
    public <T extends Serializable> boolean append(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createAppendOperation(key, value, false);
        return doBooleanSyncJob(ops, false);
    }

    public <T extends Serializable> void asyncAppend(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createAppendOperation(key, value, true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    @Override
    public <T extends Serializable> boolean prepend(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createPrependOperation(key, value, false);
        return doBooleanSyncJob(ops, false);
    }

    public <T extends Serializable> void asyncPrepend(String key, T value) {
        MemcachedOperation ops = MemcachedOperationFactory.createPrependOperation(key, value, true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    // --------------------------------------
    // Retrieval command

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Serializable> T get(String key) {
        MemcachedOperation ops = MemcachedOperationFactory.createGetOperation(key);
        return (T) doSyncJob(ObjectResultJob.class, ops);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Serializable> Map<String, T> getAll(String[] keys) {
        MemcachedOperation ops = MemcachedOperationFactory.createGetAllOperation(keys);
        return (Map<String, T>) doSyncJob(GetMultiJob.class, ops); // TODO
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Serializable> CASValue<T> gets(String key) {
        MemcachedOperation ops = MemcachedOperationFactory.createGetsOperation(key);
        return (CASValue<T>) doSyncJob(CASResultJob.class, ops);
    }

    // --------------------------------------
    // Deletion command

    @Override
    public boolean delete(String key) {
        MemcachedOperation ops = MemcachedOperationFactory.createDeleteOperation(key, false);
        return doBooleanSyncJob(ops, false);
    }

    public void asyncDelete(String key) {
        MemcachedOperation ops = MemcachedOperationFactory.createDeleteOperation(key, true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    //  --------------------------------------
    // Other commands

    @Override
    public boolean flushAll() {
        MemcachedOperation ops = MemcachedOperationFactory.createFlushOperation(false);
        return doBooleanSyncJob(ops, true);
    }

    public void asyncFlushAll() {
        MemcachedOperation ops = MemcachedOperationFactory.createFlushOperation(true);
        doAsyncJob(BooleanResultJob.class, ops);
    }

    //  --------------------------------------
    // Helper methods

    private <R> R doSyncJob(final Class<? extends GridJob<MemcachedOperation, R>> jobClass, final MemcachedOperation ops) {
        ops.setNumberOfReplicas(numberOfReplicas);
        final GridJobFuture<R> future = grid.execute(jobClass, ops);
        try {
            return future.get();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean doBooleanSyncJob(final MemcachedOperation ops, final boolean broadcast) {
        ops.setNumberOfReplicas(numberOfReplicas);
        final GridJobFuture<Boolean> future = grid.execute(broadcast ? BooleanResultBroadcastJob.class
                : BooleanResultJob.class, ops);
        try {
            return future.get().booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

    private <R> void doAsyncJob(final Class<? extends GridJob<MemcachedOperation, R>> jobClass, final MemcachedOperation ops) {
        ops.setNumberOfReplicas(numberOfReplicas);
        grid.execute(jobClass, ops);
    }

}

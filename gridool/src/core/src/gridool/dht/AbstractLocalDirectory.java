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
package gridool.dht;

import gridool.Settings;
import gridool.dht.btree.CallbackHandler;
import gridool.dht.btree.IndexException;
import gridool.locking.LockManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class AbstractLocalDirectory implements ILocalDirectory {

    protected static final boolean DELETE_IDX_ON_EXIT = Boolean.parseBoolean(Settings.get("gridool.dht.btree.delete_on_exit"));

    @Nonnull
    protected final LockManager lockManager;
    @Nonnull
    protected final ConcurrentMap<String, Integer> cacheSizes;

    public AbstractLocalDirectory(@CheckForNull LockManager lockManager) {
        if(lockManager == null) {
            throw new IllegalArgumentException();
        }
        this.lockManager = lockManager;
        this.cacheSizes = new ConcurrentHashMap<String, Integer>(16);
    }

    public void setBulkloading(boolean enable, String...idxNames) {}

    @Nonnull
    public final LockManager getLockManager() {
        return lockManager;
    }

    public final void addMapping(final byte[] key, final byte[] value) throws IndexException {
        addMapping(new byte[][] { key }, value);
    }

    public final void addMapping(final byte[][] keys, final byte[] value) throws IndexException {
        addMapping(DEFAULT_IDX_NAME, keys, value);
    }

    public final byte[][] removeMapping(byte[] key) throws IndexException {
        return removeMapping(DEFAULT_IDX_NAME, key);
    }

    public final boolean removeMapping(byte[]... keys) throws IndexException {
        return removeMapping(DEFAULT_IDX_NAME, keys);
    }

    @Nonnull
    public final void exactSearch(final byte[] key, final CallbackHandler handler) throws IndexException {
        exactSearch(DEFAULT_IDX_NAME, key, handler);
    }

    @Nonnull
    public final void prefixSearch(final byte[] key, final CallbackHandler handler)
            throws IndexException {
        prefixSearch(DEFAULT_IDX_NAME, key, handler);
    }

    public final void addMapping(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull byte[] value)
            throws IndexException {
        addMapping(idxName, new byte[][] { key }, value);
    }

    public final void setCacheSize(@Nonnull String idxName, int cacheSize) {
        cacheSizes.put(idxName, cacheSize);
    }

    @Nullable
    public final Integer getCacheSize(@Nonnull String idxName) {
        return cacheSizes.get(idxName);
    }
}
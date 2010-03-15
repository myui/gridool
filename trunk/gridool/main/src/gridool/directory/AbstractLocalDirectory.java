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
package gridool.directory;

import gridool.locking.LockManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.config.Settings;
import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class AbstractLocalDirectory implements ILocalDirectory {

    protected static final boolean DELETE_IDX_ON_EXIT = Boolean.parseBoolean(Settings.get("gridool.directory.btree.delete_on_exit"));

    @Nonnull
    protected final LockManager lockManager;
    @Nonnull
    protected final ConcurrentMap<String, Integer> cacheSizes;

    public AbstractLocalDirectory(@CheckForNull LockManager lockManger) {
        if(lockManger == null) {
            throw new IllegalArgumentException();
        }
        this.lockManager = lockManger;
        this.cacheSizes = new ConcurrentHashMap<String, Integer>(16);
    }

    public void setBulkloading(boolean enable, String...idxNames) {}

    @Nonnull
    public final LockManager getLockManager() {
        return lockManager;
    }

    public final void addMapping(final byte[] key, final byte[] value) throws DbException {
        addMapping(new byte[][] { key }, value);
    }

    public final void addMapping(final byte[][] keys, final byte[] value) throws DbException {
        addMapping(DEFAULT_IDX_NAME, keys, value);
    }

    public final byte[][] removeMapping(byte[] key) throws DbException {
        return removeMapping(DEFAULT_IDX_NAME, key);
    }

    public final boolean removeMapping(byte[]... keys) throws DbException {
        return removeMapping(DEFAULT_IDX_NAME, keys);
    }

    @Nonnull
    public final void exactSearch(final byte[] key, final BTreeCallback handler) throws DbException {
        exactSearch(DEFAULT_IDX_NAME, key, handler);
    }

    @Nonnull
    public final void prefixSearch(final byte[] key, final BTreeCallback handler)
            throws DbException {
        prefixSearch(DEFAULT_IDX_NAME, key, handler);
    }

    public final void addMapping(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull byte[] value)
            throws DbException {
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
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;
import xbird.storage.indexer.IndexQuery;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface ILocalDirectory {

    public static final String DEFAULT_IDX_NAME = "gridool";

    @Nonnull
    public LockManager getLockManager();

    @Nullable
    public <T> T getInternalIndex(@Nonnull String idxName);

    public void start() throws DbException;

    public void close() throws DbException;

    public void sync(String... idxNames) throws DbException;

    public void purgeAll(boolean clear) throws DbException;

    /**
     * @return true if created for this time. false if the specified index already exists.
     */
    public boolean create(@Nonnull String idxName) throws DbException;

    public void drop(@Nonnull String... idxNames) throws DbException;

    public void addMapping(@Nonnull byte[] key, @Nonnull byte[] value) throws DbException;

    public void addMapping(@Nonnull byte[][] keys, @Nonnull byte[] value) throws DbException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull byte[] value)
            throws DbException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[] value)
            throws DbException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[][] values)
            throws DbException;

    public void setMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[][] values)
            throws DbException;

    @Nullable
    public byte[][] removeMapping(@Nonnull byte[] key) throws DbException;

    @Nullable
    public byte[][] removeMapping(@Nonnull String idxName, @Nonnull byte[] key) throws DbException;

    public boolean removeMapping(@Nonnull byte[]... keys) throws DbException;

    public boolean removeMapping(@Nonnull String idxName, @Nonnull byte[]... keys)
            throws DbException;

    public void prefixSearch(@Nonnull byte[] key, @Nonnull BTreeCallback handler)
            throws DbException;

    public void prefixSearch(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull BTreeCallback handler)
            throws DbException;

    public void exactSearch(@Nonnull byte[] key, @Nonnull BTreeCallback handler) throws DbException;

    public void exactSearch(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull BTreeCallback handler)
            throws DbException;

    public void retrieve(@Nonnull String idxName, @Nonnull IndexQuery query, @Nonnull BTreeCallback callback)
            throws DbException;

    @Nullable
    public byte[] getValue(@Nonnull String idxName, @Nonnull byte[] key) throws DbException;

    public void setCacheSize(@Nonnull String idxName, int cacheSize);

    public Integer getCacheSize(@Nonnull String idxName);

    public enum DirectoryIndexType {
        bfile /* default */, tcb, tch;

        public static DirectoryIndexType resolve(String type) {
            if("tcb".equalsIgnoreCase(type)) {
                return tcb;
            } else if("tch".equalsIgnoreCase(type)) {
                return tch;
            } else if("bfile".equals(type)) {
                return bfile;
            } else {
                return bfile;
            }
        }
    }

}
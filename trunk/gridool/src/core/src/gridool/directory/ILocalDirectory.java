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

import gridool.directory.btree.CallbackHandler;
import gridool.directory.btree.IndexException;
import gridool.directory.btree.IndexQuery;
import gridool.locking.LockManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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

    public void setBulkloading(boolean enable, String... idxNames);

    public void start() throws IndexException;

    public void close() throws IndexException;

    public void sync(String... idxNames) throws IndexException;

    public void purgeAll(boolean clear) throws IndexException;

    /**
     * @return true if created for this time. false if the specified index already exists.
     */
    public boolean create(@Nonnull String idxName) throws IndexException;

    public void drop(@Nonnull String... idxNames) throws IndexException;

    public void addMapping(@Nonnull byte[] key, @Nonnull byte[] value) throws IndexException;

    public void addMapping(@Nonnull byte[][] keys, @Nonnull byte[] value) throws IndexException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull byte[] value)
            throws IndexException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[] value)
            throws IndexException;

    public void addMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[][] values)
            throws IndexException;

    public void setMapping(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[][] values)
            throws IndexException;

    @Nullable
    public byte[][] removeMapping(@Nonnull byte[] key) throws IndexException;

    @Nullable
    public byte[][] removeMapping(@Nonnull String idxName, @Nonnull byte[] key) throws IndexException;

    public boolean removeMapping(@Nonnull byte[]... keys) throws IndexException;

    public boolean removeMapping(@Nonnull String idxName, @Nonnull byte[]... keys)
            throws IndexException;

    public void prefixSearch(@Nonnull byte[] key, @Nonnull CallbackHandler handler)
            throws IndexException;

    public void prefixSearch(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull CallbackHandler handler)
            throws IndexException;

    public void exactSearch(@Nonnull byte[] key, @Nonnull CallbackHandler handler)
            throws IndexException;

    public void exactSearch(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull CallbackHandler handler)
            throws IndexException;

    public void retrieve(@Nonnull String idxName, @Nonnull IndexQuery query, @Nonnull CallbackHandler callback)
            throws IndexException;

    public void setCacheSize(@Nonnull String idxName, int cacheSize);

    public Integer getCacheSize(@Nonnull String idxName);

    public enum DirectoryIndexType {
        bfile /* default */, tcb, tch, bdb;

        public static DirectoryIndexType resolve(String type) {
            if("bfile".equalsIgnoreCase(type)) {
                return bfile;
            } else if("tcb".equalsIgnoreCase(type)) {
                return tcb;
            } else if("tch".equalsIgnoreCase(type)) {
                return tch;
            } else if("bdb".equalsIgnoreCase(type)) {
                return bdb;
            } else {
                return bfile;
            }
        }
    }

}
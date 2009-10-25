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

import gridool.GridConfiguration;
import gridool.memcached.cache.MCElement;
import gridool.memcached.cache.MCElementSnapshot;
import xbird.util.cache.ICacheEntry;
import xbird.util.concurrent.cache.ConcurrentPluggableCache;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MemcachedDaemon implements Memcached {

    private final ConcurrentPluggableCache<String, MCElement> cache_;

    public MemcachedDaemon(GridConfiguration config) {
        int cacheSize = config.getMemcachedCacheSize();
        this.cache_ = new ConcurrentPluggableCache<String, MCElement>(cacheSize);
    }

    @Override
    public void stop() {
        cache_.clear();
    }

    // --------------------------------------

    @Override
    public MCElementSnapshot prepareOperation(String key, boolean writeOps) {
        final ICacheEntry<String, MCElement> entry = cache_.fixEntry(key);
        try {
            final MCElement elem = entry.getValue();
            if(elem != null) {
                boolean locked = elem.tryLock(writeOps);
                Serializable[] values = elem.getValues();
                long version = elem.getVersion();
                return new MCElementSnapshot(values, version, locked);
            }
            return null;
        } finally {
            entry.unpin();
        }
    }

    @Override
    public void abortOperation(String key, boolean writeOps) {
        final MCElement elem = cache_.get(key);
        if(elem != null) {
            elem.unlock(writeOps);
        }
    }

    @Override
    public <T extends Serializable> void commitOperation(String key, T value, boolean writeOps) {

    }

    // --------------------------------------
    // Storage command

    @Override
    public <T extends Serializable> boolean set(String key, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> boolean add(String key, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> Serializable replace(String key, T value) {
        throw new UnsupportedOperationException();
    }

    // --------------------------------------
    // Additional storage command

    @Override
    public <T extends Serializable> Serializable cas(String key, long casId, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> boolean append(String key, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> boolean prepend(String key, T value) {
        throw new UnsupportedOperationException();
    }

    // --------------------------------------
    // Retrieval command

    @Override
    public <T extends Serializable> T get(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> Map<String, T> getAll(String[] keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Serializable> CASValue<T> gets(String key) {
        throw new UnsupportedOperationException();
    }

    // --------------------------------------
    // Deletion command

    @Override
    public boolean delete(String key) {
        throw new UnsupportedOperationException();
    }

    //  --------------------------------------
    // Other commands

    @Override
    public boolean flushAll() {
        throw new UnsupportedOperationException();
    }

}

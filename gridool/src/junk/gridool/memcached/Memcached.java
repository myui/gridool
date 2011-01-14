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
import javax.annotation.Nullable;

import gridool.memcached.cache.MCElementSnapshot;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface Memcached {

    @Nullable
    MCElementSnapshot prepareOperation(String key, boolean writeOps);

    void abortOperation(String key, boolean writeOps);

    <T extends Serializable> void commitOperation(String key, T value, boolean writeOps);

    // --------------------------------------
    // Storage command

    /**
     * @return <tt>true</tt> if this set did not already contain the specified element
     */
    <T extends Serializable> boolean set(String key, T value);

    /**
     * Sets if no entry found for the specified key.
     */
    <T extends Serializable> boolean add(String key, T value);

    <T extends Serializable> Serializable replace(String key, T value);

    // --------------------------------------
    // Additional storage command

    /**
     * A check and set operation which means "store this data but 
     * only if no one else has updated since I last fetched it."
     */
    <T extends Serializable> Serializable cas(String key, long casId, T value);

    <T extends Serializable> boolean append(String key, T value);

    <T extends Serializable> boolean prepend(String key, T value);

    // --------------------------------------
    // Retrieval command

    <T extends Serializable> T get(String key);

    <T extends Serializable> Map<String, T> getAll(String[] keys);

    @Nonnull
    <T extends Serializable> CASValue<T> gets(String key);

    // --------------------------------------
    // Deletion command

    boolean delete(String key);

    //  --------------------------------------
    // Other commands

    boolean flushAll();

    void stop();

}

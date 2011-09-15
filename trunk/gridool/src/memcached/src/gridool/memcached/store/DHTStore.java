/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.memcached.store;

import gridool.GridResourceRegistry;
import gridool.dht.ILocalDirectory;
import gridool.dht.btree.IndexException;
import gridool.memcached.MemcachedCommandHandler;
import gridool.memcached.binary.BinaryProtocol.ResponseStatus;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTStore implements MemcachedCommandHandler {

    private final ILocalDirectory dir;

    public DHTStore(@Nonnull GridResourceRegistry registry) {
        this.dir = registry.getDirectory();
    }

    @Override
    public byte[] handleGet(final byte[] key) {
        try {
            return dir.get(key);
        } catch (IndexException e) {
            return null;
        }
    }

    @Override
    public short handleSet(final byte[] key, final byte[] value, final int flags, final int expiry) {
        try {
            dir.set(key, value);
        } catch (IndexException e) {
            return ResponseStatus.INTERNAL_ERROR.status;
        }
        return ResponseStatus.NO_ERROR.status;
    }

}

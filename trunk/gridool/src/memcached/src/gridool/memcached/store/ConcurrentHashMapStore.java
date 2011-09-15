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

import gridool.memcached.MemcachedCommandHandler;
import gridool.memcached.binary.BinaryProtocol.ResponseStatus;
import gridool.util.struct.ByteArray;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ConcurrentHashMapStore implements MemcachedCommandHandler {

    private final Map<ByteArray, byte[]> map;

    public ConcurrentHashMapStore() {
        map = new ConcurrentHashMap<ByteArray, byte[]>(1024);
    }

    @Override
    public byte[] handleGet(byte[] key) {
        return map.get(new ByteArray(key));
    }

    @Override
    public short handleSet(byte[] key, byte[] value, int flags, int expiry) {
        map.put(new ByteArray(key), value);
        return ResponseStatus.NO_ERROR.status;
    }

}

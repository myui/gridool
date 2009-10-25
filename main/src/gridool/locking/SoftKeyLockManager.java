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
package gridool.locking;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import xbird.util.concurrent.jsr166.ConcurrentReferenceHashMap;
import xbird.util.concurrent.jsr166.ConcurrentReferenceHashMap.ReferenceType;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class SoftKeyLockManager implements LockManager {

    private final ConcurrentMap<Object, ReadWriteLock> lockPool;

    public SoftKeyLockManager() {
        this.lockPool = new ConcurrentReferenceHashMap<Object, ReadWriteLock>(32, ReferenceType.SOFT, ReferenceType.STRONG);
    }

    public ReadWriteLock obtainLock(final Object resource) {
        ReadWriteLock lock = lockPool.get(resource);
        if(lock == null) {
            final ReadWriteLock newLock = new ReentrantReadWriteLock();
            final ReadWriteLock prevLock = lockPool.putIfAbsent(resource, newLock);
            if(prevLock == null) {
                lock = newLock;
            } else {
                lock = prevLock;
            }
        }
        return lock;
    }

}

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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class VacuumingLockManager implements LockManager {

    private final ConcurrentMap<Object, ThrowawayReadWriteLock> lockPool;

    public VacuumingLockManager() {
        this.lockPool = new ConcurrentHashMap<Object, ThrowawayReadWriteLock>();
    }

    public ReadWriteLock obtainLock(@Nonnull Object resource) {
        ThrowawayReadWriteLock lock = lockPool.get(resource);
        if(lock == null) {
            final ThrowawayReadWriteLock newLock = new ThrowawayReadWriteLock(resource, lockPool);
            final ThrowawayReadWriteLock prevLock = lockPool.putIfAbsent(resource, newLock);
            if(prevLock == null) {
                lock = newLock;
            } else {
                lock = prevLock;
            }
        }
        return lock;
    }

    private static final class ThrowawayReadWriteLock implements ReadWriteLock {

        private final Lock readLock;
        private final Lock writeLock;
        private final Object key;

        private final ConcurrentMap<Object, ThrowawayReadWriteLock> holder;
        private final AtomicInteger lockAcquired;

        public ThrowawayReadWriteLock(Object key, ConcurrentMap<Object, ThrowawayReadWriteLock> holder) {
            ReadWriteLock delegate = new ReentrantReadWriteLock();
            Lock rlock = delegate.readLock();
            this.readLock = new ThrowawayLockAdapter(rlock, this);
            Lock wlock = delegate.writeLock();
            this.writeLock = new ThrowawayLockAdapter(wlock, this);
            this.key = key;
            this.holder = holder;
            this.lockAcquired = new AtomicInteger(0);
        }

        public Lock readLock() {
            return readLock;
        }

        public Lock writeLock() {
            return writeLock;
        }

    }

    private static final class ThrowawayLockAdapter implements Lock {

        private final Lock delegate;

        private final Object key;
        private final ThrowawayReadWriteLock value;

        private final ConcurrentMap<Object, ThrowawayReadWriteLock> holder;
        private final AtomicInteger lockAcquired;

        public ThrowawayLockAdapter(Lock delegate, ThrowawayReadWriteLock adapter) {
            this.delegate = delegate;
            this.key = adapter.key;
            this.value = adapter;
            this.holder = adapter.holder;
            this.lockAcquired = adapter.lockAcquired;
        }

        public void lock() {
            lockAcquired.getAndIncrement();
            delegate.lock();
        }

        public void lockInterruptibly() throws InterruptedException {
            lockAcquired.getAndIncrement();
            try {
                delegate.lockInterruptibly();
            } catch (InterruptedException e) {
                lockAcquired.getAndDecrement();
                throw e;
            }
        }

        public boolean tryLock() {
            lockAcquired.getAndIncrement();
            if(!delegate.tryLock()) {
                lockAcquired.getAndDecrement();
                return false;
            }
            return true;
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            lockAcquired.getAndIncrement();
            if(!delegate.tryLock(time, unit)) {
                lockAcquired.getAndDecrement();
                return false;
            }
            return true;
        }

        public void unlock() {
            delegate.unlock();
            // post condition: remove lock-binding if this lock is unused.
            if(lockAcquired.decrementAndGet() <= 0) { // considering <0 for unlock -> unlock
                holder.remove(key, value);
            }
        }

        public Condition newCondition() {
            return delegate.newCondition();
        }

    }

}

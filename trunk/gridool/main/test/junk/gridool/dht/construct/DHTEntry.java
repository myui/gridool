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
package gridool.dht.construct;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTEntry implements Externalizable {
    private static final long serialVersionUID = 5696677939894814437L;

    @GuardedBy("rwlock")
    private final List<Serializable> values;
    private transient final ReadWriteLock rwlock;

    public DHTEntry() {
        this.values = new ArrayList<Serializable>(2);
        this.rwlock = new ReentrantReadWriteLock();
    } // for Externalizable

    public <T extends Serializable> DHTEntry(T value) {
        this.values = new ArrayList<Serializable>(2);
        values.add(value);
        this.rwlock = new ReentrantReadWriteLock();
    }

    public void lock(boolean writeOps) {
        if(writeOps) {
            rwlock.writeLock().lock();
        } else {
            rwlock.readLock().lock();
        }
    }

    public boolean tryLock(boolean writeOps) {
        if(writeOps) {
            return rwlock.writeLock().tryLock();
        } else {
            return rwlock.readLock().tryLock();
        }
    }

    public void unlock(boolean writeOps) {
        if(writeOps) {
            rwlock.writeLock().unlock();
        } else {
            rwlock.readLock().unlock();
        }
    }

    public <T extends Serializable> void addValue(T value) {
        synchronized(values) {
            if(!values.contains(value)) {
                values.add(value);
            }
        }
    }

    public <T extends Serializable> void setValue(T value) {
        synchronized(values) {
            values.clear();
            values.add(value);
        }
    }

    public <T extends Serializable> boolean contains(T value) {
        synchronized(values) {
            return values.contains(value);
        }
    }

    /**
     * Returns the latest values.
     */
    @Nonnull
    public <T extends Serializable> List<T> getValues() {
        final T[] ary = values();
        return Arrays.asList(ary);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        for(int i = 0; i < len; i++) {
            Object e = in.readObject();
            values.add((Serializable) e);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final Serializable[] ary = values();
        out.writeInt(ary.length);
        for(Serializable e : ary) {
            out.writeObject(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T[] values() {
        final Serializable[] ary;
        synchronized(values) {
            ary = new Serializable[values.size()];
            values.toArray(ary);
        }
        return (T[]) ary;
    }

}

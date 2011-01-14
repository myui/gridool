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
package gridool.memcached.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MCElement implements Externalizable {
    private static final long serialVersionUID = 5696677939894814437L;
    public static final long INITIAL_CAS_ID = 0;

    private final AtomicLong version;
    private final List<Serializable> values;
    private ReadWriteLock rwlock;

    public MCElement() {
        this.version = new AtomicLong(-1);
        this.values = new ArrayList<Serializable>(2);
    } // for Externalizable

    public MCElement(Serializable value) {
        this.version = new AtomicLong(INITIAL_CAS_ID);
        this.values = new ArrayList<Serializable>(2);
        values.add(value);
        this.rwlock = new ReentrantReadWriteLock();
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

        }
    }

    public long getVersion() {
        return version.get();
    }

    public Serializable[] getValues() {
        return (Serializable[]) values.toArray();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        long ver = in.readLong();
        version.set(ver);
        final int len = in.readInt();
        for(int i = 0; i < len; i++) {
            Serializable e = (Serializable) in.readObject();
            values.add(e);
        }
        this.rwlock = (ReadWriteLock) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(version.get());
        out.writeInt(values.size());
        for(Serializable v : values) {
            out.writeObject(v);
        }
        out.writeObject(rwlock);
    }

}

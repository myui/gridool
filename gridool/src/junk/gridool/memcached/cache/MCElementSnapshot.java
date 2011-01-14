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

import gridool.GridNode;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MCElementSnapshot implements Externalizable {
    private static final long serialVersionUID = -1554593880699468253L;

    private Serializable[] values;
    private long version;
    private boolean locked;

    private transient GridNode residentNode = null;

    public MCElementSnapshot() {}// for Externalizable

    public MCElementSnapshot(Serializable[] values, long version, boolean locked) {
        this.values = values;
        this.version = version;
        this.locked = locked;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T[] getValues() {
        return (T[]) values;
    }

    public long getVersion() {
        return version;
    }

    public boolean isLockAcquired() {
        return locked;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        this.values = new Serializable[len];
        for(int i = 0; i < len; i++) {
            values[i] = (Serializable) in.readObject();
        }
        this.version = in.readLong();
        this.locked = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(values.length);
        for(Serializable v : values) {
            out.writeObject(v);
        }
        out.writeLong(version);
        out.writeBoolean(locked);
    }

    public GridNode getResidentNode() {
        return residentNode;
    }

    public void setResidentNode(GridNode residentNode) {
        this.residentNode = residentNode;
    }

}

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

import javax.annotation.Nonnull;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTEntrySnapshot implements Externalizable {
    private static final long serialVersionUID = -1554593880699468253L;

    private String key;
    @Nonnull
    private ValueWithVersion[] values;
    private boolean locked;

    public DHTEntrySnapshot() {}// for Externalizable

    public DHTEntrySnapshot(String key, ValueWithVersion[] values, boolean locked) {
        this.key = key;
        this.values = values;
        this.locked = locked;
    }

    public String getKey() {
        return key;
    }
    
    public ValueWithVersion[] getValues() {
        return values;
    }

    public boolean isLockAcquired() {
        return locked;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.key = IOUtils.readString(in);
        final int len = in.readInt();
        this.values = new ValueWithVersion[len];
        for(int i = 0; i < len; i++) {
            values[i] = ValueWithVersion.readObject(in);
        }
        this.locked = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, key);
        out.writeInt(values.length);
        for(ValueWithVersion v : values) {
            v.writeExternal(out);
        }
        out.writeBoolean(locked);
    }

    public static DHTEntrySnapshot readObject(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final DHTEntrySnapshot e = new DHTEntrySnapshot();
        e.readExternal(in);
        return e;
    }

}

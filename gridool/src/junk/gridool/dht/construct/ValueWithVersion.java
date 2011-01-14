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

import gridool.GridNode;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ValueWithVersion implements Externalizable {
    private static final long serialVersionUID = -4423648688967007795L;
    private static final long INIT_VERSION = 0L;

    private/* final */Serializable value;
    private/* final */long version;

    private transient GridNode residentNode = null;

    public ValueWithVersion() {}//Externalizable

    public ValueWithVersion(Serializable value) {
        this.value = value;
        this.version = INIT_VERSION;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T getValue() {
        return (T) value;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.value = (Serializable) in.readObject();
        this.version = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(value);
        out.writeLong(version);
    }

    public static ValueWithVersion readObject(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final ValueWithVersion value = new ValueWithVersion();
        value.readExternal(in);
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(obj instanceof ValueWithVersion) {
            Serializable otherValue = ((ValueWithVersion) obj).value;
            return value.equals(otherValue);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    public GridNode getResidentNode() {
        return residentNode;
    }

    public void setResidentNode(GridNode residentNode) {
        this.residentNode = residentNode;
    }

}

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
package gridool.deployment;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ClassData implements Externalizable {
    private static final long serialVersionUID = -752396835747683781L;

    @Nullable
    private String clsName;
    @Nonnull
    private byte[] clazz;
    @Nonnegative
    private long timestamp;
    @Nullable
    private ClassData enclosingClass;

    public ClassData() {} // for Externalizable

    public ClassData(@Nonnull byte[] b, @Nonnegative long timestamp) {
        this(null, b, timestamp);
    }

    public ClassData(@Nullable String clsName, @Nonnull byte[] b, @Nonnegative long timestamp) {
        this.clsName = clsName;
        this.clazz = b;
        this.timestamp = timestamp;
    }

    public String getClassName() {
        return clsName;
    }

    public byte[] getClassData() {
        return clazz;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setEnclosingClass(@Nonnull ClassData clazz) {
        this.enclosingClass = clazz;
    }

    @Nullable
    public ClassData getEnclosingClass() {
        return enclosingClass;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.clsName = IOUtils.readString(in);
        this.clazz = IOUtils.readBytes(in);
        this.timestamp = in.readLong();
        if(in.readBoolean()) {
            this.enclosingClass = readFrom(in);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(clsName, out);
        IOUtils.writeBytes(clazz, out);
        out.writeLong(timestamp);
        if(enclosingClass == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            enclosingClass.writeExternal(out);
        }
    }

    private static ClassData readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
        ClassData data = new ClassData();
        data.readExternal(in);
        return data;
    }

}

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
package gridool.db.catalog;

import gridool.db.helpers.ConstraintKey;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class PartitionKey implements Externalizable {

    private/* final */boolean isPrimary;
    private/* final */int partitionNo;

    private transient ConstraintKey key;

    public PartitionKey() {} // Externalizable

    public PartitionKey(@CheckForNull ConstraintKey key, int partitionNo) {
        if(key == null) {
            throw new IllegalArgumentException();
        }
        this.isPrimary = key.isPrimaryKey();
        this.partitionNo = partitionNo;
        this.key = key;
    }

    public PartitionKey(boolean isPrimary, int partitionNo) {
        this.isPrimary = isPrimary;
        this.partitionNo = partitionNo;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T extends ConstraintKey> T getKey() {
        return (T) key;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    @Override
    public String toString() {
        return (isPrimary ? "PK:" : "FK:") + " partition #" + partitionNo;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.isPrimary = in.readBoolean();
        this.partitionNo = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isPrimary);
        out.writeInt(partitionNo);
    }

}
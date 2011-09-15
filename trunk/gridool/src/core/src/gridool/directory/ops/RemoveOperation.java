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
package gridool.directory.ops;

import gridool.GridException;
import gridool.directory.ILocalDirectory;
import gridool.directory.btree.IndexException;
import gridool.util.collections.ImmutableArrayList;
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class RemoveOperation implements DirectoryOperation {

    private/* final */String idxName;
    private/* final */List<byte[]> keys;
    private boolean async = false;

    public RemoveOperation() {}//for Externalizable

    /**
     * @param keys fixed size keys
     */
    public RemoveOperation(@Nonnull byte[]... keys) {
        this(ILocalDirectory.DEFAULT_IDX_NAME, Arrays.asList(keys));
    }

    public RemoveOperation(@Nonnull List<byte[]> keys) {
        this(ILocalDirectory.DEFAULT_IDX_NAME, keys);
    }

    public RemoveOperation(@Nonnull String idxName) {
        this(idxName, new ArrayList<byte[]>(8));
    }

    /**
     * @param keys fixed size keys
     */
    public RemoveOperation(@Nonnull String idxName, @Nonnull byte... keys) {
        this.idxName = idxName;
        this.keys = Arrays.asList(keys);
    }

    public RemoveOperation(@Nonnull String idxName, @Nonnull List<byte[]> keys) {
        this.idxName = idxName;
        this.keys = keys;
    }

    public boolean isAsyncOps() {
        return async;
    }

    public void setAsyncOps(boolean async) {
        this.async = async;
    }

    public void addRemoveKey(byte[]... keylist) {
        for(byte[] k : keylist) {
            keys.add(k);
        }
    }

    @SuppressWarnings("unchecked")
    public byte[][] getKeys() {
        if(keys instanceof ImmutableArrayList) {
            return ((ImmutableArrayList<byte[]>) keys).getInternalArray();
        }
        final byte[][] b = new byte[keys.size()][];
        return keys.toArray(b);
    }

    public Boolean execute(ILocalDirectory directory) throws GridException {
        final byte[][] keys = getKeys();
        final boolean removedAll;
        try {
            removedAll = directory.removeMapping(idxName, keys);
        } catch (IndexException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
        return Boolean.valueOf(removedAll);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.idxName = IOUtils.readString(in);
        int size = in.readInt();
        final byte[][] ary = new byte[size][];
        for(int i = 0; i < size; i++) {
            ary[i] = IOUtils.readBytes(in);
        }
        this.keys = new ImmutableArrayList<byte[]>(ary);
        this.async = in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(idxName, out);
        out.writeInt(keys.size());
        for(byte[] b : keys) {
            IOUtils.writeBytes(b, out);
        }
        out.writeBoolean(async);
    }

    public RemoveOperation makeOperation(final List<byte[]> keys) {
        return new RemoveOperation(idxName, keys);
    }

}

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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@NotThreadSafe
public class AddOperation implements DirectoryOperation {

    private/* final */String idxName;
    private/* final */List<byte[]> keys;
    private/* final */List<byte[]> values;

    private boolean async = false;
    private int maxNumReplicas = 0;

    public AddOperation() {}//for Externalizable

    public AddOperation(@Nonnull String idxName) {
        this.idxName = idxName;
        this.keys = new ArrayList<byte[]>(4);
        this.values = new ArrayList<byte[]>(4);
    }

    public AddOperation(@Nonnull byte[] key, @Nonnull byte[] value) {
        this(ILocalDirectory.DEFAULT_IDX_NAME, key, value);
    }

    public AddOperation(@Nonnull String idxName, @Nonnull byte[] key, @Nonnull byte[] value) {
        this.idxName = idxName;
        this.keys = new ArrayList<byte[]>(4);
        this.values = new ArrayList<byte[]>(4);
        keys.add(key);
        values.add(value);
    }

    /**
     * Fixed size add operation. 
     * Not that adding keys/values is not allowed when using this constructor.
     */
    public AddOperation(@Nonnull String idxName, @Nonnull byte[][] keys, @Nonnull byte[][] values) {
        this.idxName = idxName;
        this.keys = Arrays.asList(keys);
        this.values = Arrays.asList(values);
    }

    public AddOperation(@Nonnull String idxName, @Nonnull List<byte[]> keys, @Nonnull List<byte[]> values) {
        this.idxName = idxName;
        this.keys = keys;
        this.values = values;
    }

    public boolean isAsyncOps() {
        return async;
    }

    public void setAsyncOps(boolean async) {
        this.async = async;
    }

    public int getMaxNumReplicas() {
        return maxNumReplicas;
    }

    public void setMaxNumReplicas(@Nonnegative int replicas) {
        this.maxNumReplicas = replicas;
    }

    public String getName() {
        return idxName;
    }

    @SuppressWarnings("unchecked")
    public byte[][] getKeys() {
        if(keys instanceof ImmutableArrayList) {
            return ((ImmutableArrayList<byte[]>) keys).getInternalArray();
        }
        final byte[][] b = new byte[keys.size()][];
        return keys.toArray(b);
    }

    @SuppressWarnings("unchecked")
    public byte[][] getValues() {
        if(values instanceof ImmutableArrayList) {
            return ((ImmutableArrayList<byte[]>) values).getInternalArray();
        }
        final byte[][] b = new byte[values.size()][];
        return values.toArray(b);
    }

    public void addMapping(@Nonnull byte[] key, @Nonnull byte[] value) {
        keys.add(key);
        values.add(value);
    }

    public Serializable execute(final ILocalDirectory directory) throws GridException {
        final byte[][] keys = getKeys();
        final byte[][] values = getValues();
        try {
            directory.addMapping(idxName, keys, values);
        } catch (IndexException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.idxName = IOUtils.readString(in);
        final int size = in.readInt();
        final byte[][] lkeys = new byte[size][];
        final byte[][] lvalues = new byte[size][];
        for(int i = 0; i < size; i++) {
            lkeys[i] = IOUtils.readBytes(in);
            lvalues[i] = IOUtils.readBytes(in);
        }
        this.keys = new ImmutableArrayList<byte[]>(lkeys);
        this.values = new ImmutableArrayList<byte[]>(lvalues);
        this.async = in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(idxName, out);
        final int size = keys.size();
        out.writeInt(size);
        for(int i = 0; i < size; i++) {
            byte[] k = keys.get(i);
            byte[] v = values.get(i);
            IOUtils.writeBytes(k, out);
            IOUtils.writeBytes(v, out);
        }
        out.writeBoolean(async);
    }

    public AddOperation makeOperation(List<byte[]> keys, List<byte[]> values) {
        return new AddOperation(idxName, keys, values);
    }

}

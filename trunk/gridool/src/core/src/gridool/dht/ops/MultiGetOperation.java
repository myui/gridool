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
package gridool.dht.ops;

import gridool.GridException;
import gridool.dht.ILocalDirectory;
import gridool.dht.btree.IndexException;
import gridool.dht.helpers.GenericValueCollector;
import gridool.util.collections.ImmutableArrayList;
import gridool.util.converter.NoopConverter;
import gridool.util.io.IOUtils;
import gridool.util.string.StringUtils;
import gridool.util.struct.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
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
public final class MultiGetOperation implements DirectoryOperation {

    private/* final */String _idxName;
    private/* final */List<byte[]> _keys;

    public MultiGetOperation() {}// for Externalizable

    public MultiGetOperation(@Nonnull String idxName, @Nonnull byte[]... keys) {
        this._idxName = idxName;
        int expected = (int) (keys.length * 1.4);
        List<byte[]> keyList = new ArrayList<byte[]>(expected);
        for(byte[] k : keys) {
            keyList.add(k);
        }
        this._keys = keyList;
    }

    public MultiGetOperation(@Nonnull String idxName, @Nonnull List<byte[]> keys) {
        this._idxName = idxName;
        this._keys = keys;
    }

    public boolean isAsyncOps() {
        return false;
    }

    public String getIndexName() {
        return _idxName;
    }

    public void addKeys(byte[]... keys) {
        for(byte[] k : keys) {
            _keys.add(k);
        }
    }

    @SuppressWarnings("unchecked")
    public byte[][] getKeys() {
        if(_keys instanceof ImmutableArrayList) {
            return ((ImmutableArrayList<byte[]>) _keys).getInternalArray();
        }
        final byte[][] b = new byte[_keys.size()][];
        return _keys.toArray(b);
    }

    @SuppressWarnings("unchecked")
    public Pair<byte[], byte[][]>[] execute(ILocalDirectory directory) throws GridException {
        final int size = _keys.size();
        final Pair<byte[], byte[][]>[] values = new Pair[size];
        final GenericValueCollector<byte[]> collector = new GenericValueCollector<byte[]>(NoopConverter.getInstance());
        for(int i = 0; i < size; i++) {
            final byte[] key = _keys.get(i);
            try {
                directory.exactSearch(_idxName, key, collector);
            } catch (IndexException e) {
                String errmsg = "Exception caused while searching key '"
                        + StringUtils.toString(key) + "' on index '" + _idxName + '\'';
                LogFactory.getLog(getClass()).error(errmsg, e);
                throw new GridException(errmsg, e);
            }
            final List<byte[]> list = collector.getResult();
            final byte[][] v = new byte[list.size()][];
            list.toArray(v);
            values[i] = new Pair<byte[], byte[][]>(key, v);
            collector.clear();
        }
        return values;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(_idxName, out);
        final int keylen = _keys.size();
        out.writeInt(keylen);
        for(int i = 0; i < keylen; i++) {
            byte[] k = _keys.get(i);
            IOUtils.writeBytes(k, out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._idxName = IOUtils.readString(in);
        final int size = in.readInt();
        final byte[][] lkeys = new byte[size][];
        for(int i = 0; i < size; i++) {
            lkeys[i] = IOUtils.readBytes(in);
        }
        this._keys = new ImmutableArrayList<byte[]>(lkeys);
    }

}

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
import gridool.communication.payload.GridNodeInfo;
import gridool.directory.ILocalDirectory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;
import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class AddGridNodeOperation implements DirectoryOperation {

    @Nonnull
    private/* final */byte[][] keys;
    @Nonnull
    private/* final */GridNodeInfo node;

    public AddGridNodeOperation() {}// for Externalizable

    public AddGridNodeOperation(@Nonnull byte[][] keys, @Nonnull GridNodeInfo node) {
        assert (keys != null);
        assert (node != null);
        this.keys = keys;
        this.node = node;
    }

    public boolean isAsyncOps() {
        return true;
    }

    public byte[][] getKeys() {
        return keys;
    }

    public GridNodeInfo getValue() {
        return node;
    }

    public Serializable execute(ILocalDirectory directory) throws GridException {
        final byte[] v = node.toBytes(false);
        try {
            directory.addMapping(keys, v);
        } catch (DbException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException("Exception caused while adding a mapping", e);
        }
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        final byte[][] k = new byte[len][];
        for(int i = 0; i < len; i++) {
            k[i] = IOUtils.readBytes(in);
        }
        this.keys = k;
        this.node = GridNodeInfo.readFrom(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(keys.length);
        for(byte[] k : keys) {
            IOUtils.writeBytes(k, out);
        }
        node.writeExternal(out);
    }

    public AddGridNodeOperation makeOperation(byte[][] mappedKeys) {
        return new AddGridNodeOperation(mappedKeys, node);
    }

}

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
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DropOperation implements DirectoryOperation {

    @Nonnull
    private String[] idxNames;

    private boolean async = false;

    public DropOperation() {}//for Externalizable

    public DropOperation(@Nonnull String... idxNames) {
        this.idxNames = idxNames;
    }

    public byte[][] getKeys() {
        throw new UnsupportedOperationException();
    }

    public void setAsyncOps(boolean async) {
        this.async = async;
    }

    public boolean isAsyncOps() {
        return async;
    }

    public Serializable execute(ILocalDirectory directory) throws GridException {
        if(idxNames.length > 0) {
            try {
                directory.drop(idxNames);
            } catch (IndexException e) {
                LogFactory.getLog(getClass()).error(e.getMessage(), e);
                throw new GridException(e);
            }
        }
        return null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        final int len = idxNames.length;
        out.writeInt(len);
        for(int i = 0; i < len; i++) {
            IOUtils.writeString(idxNames[i], out);
        }
        out.writeBoolean(async);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        final String[] idxNames = new String[len];
        for(int i = 0; i < len; i++) {
            idxNames[i] = IOUtils.readString(in);
        }
        this.idxNames = idxNames;
        this.async = in.readBoolean();
    }

}

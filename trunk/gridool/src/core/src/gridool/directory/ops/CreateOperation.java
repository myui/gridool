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
import gridool.util.io.IOUtils;
import gridool.util.string.StringUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;


/**
 * CreateOperation return false when the specified index already exists on some nodes.
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CreateOperation implements DirectoryOperation {

    @Nonnull
    private String idxName;

    public CreateOperation() {} // for Externalizable

    public CreateOperation(String idxName) {
        this.idxName = idxName;
    }

    public byte[][] getKeys() {
        return new byte[][] { StringUtils.getBytes(idxName) };
    }

    public boolean isAsyncOps() {
        return false;
    }

    public Boolean execute(ILocalDirectory directory) throws GridException {
        try {
            return directory.create(idxName);
        } catch (IndexException e) {
            LogFactory.getLog(CreateOperation.class).error(e.getMessage(), e);
            return false;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(idxName, out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.idxName = IOUtils.readString(in);
    }

}

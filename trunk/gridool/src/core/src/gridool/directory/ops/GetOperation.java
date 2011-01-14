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
import gridool.directory.helpers.GenericValueCollector;
import gridool.util.converter.NoopConverter;
import gridool.util.io.IOUtils;
import gridool.util.string.StringUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
public class GetOperation implements DirectoryOperation {

    private/* final */String idxName;
    private/* final */byte[] key;

    public GetOperation() {}// for Externalizable

    public GetOperation(@Nonnull String idxName, @Nonnull byte[] key) {
        this.idxName = idxName;
        this.key = key;
    }

    public boolean isAsyncOps() {
        return false;
    }

    public byte[][] getKeys() {
        return new byte[][] { key };
    }

    public byte[][] execute(ILocalDirectory directory) throws GridException {
        final GenericValueCollector<byte[]> collector = new GenericValueCollector<byte[]>(NoopConverter.getInstance());
        try {
            directory.exactSearch(idxName, key, collector);
        } catch (IndexException e) {
            String errmsg = "Exception caused while searching key '" + StringUtils.toString(key)
                    + "' on index '" + idxName + '\'';
            LogFactory.getLog(getClass()).error(errmsg, e);
            throw new GridException(errmsg, e);
        }
        final List<byte[]> list = collector.getResult();
        final byte[][] ary = new byte[list.size()][];
        return list.toArray(ary);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(idxName, out);
        IOUtils.writeBytes(key, out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.idxName = IOUtils.readString(in);
        this.key = IOUtils.readBytes(in);
    }

}

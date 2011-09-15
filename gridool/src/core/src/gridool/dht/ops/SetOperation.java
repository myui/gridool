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

import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class SetOperation extends AddOperation {

    public SetOperation() {}//for Externalizable

    public SetOperation(String idxName) {
        super(idxName);
    }

    public SetOperation(byte[] key, byte[] value) {
        super(key, value);
    }

    public SetOperation(String idxName, byte[] key, byte[] value) {
        super(idxName, key, value);
    }

    /**
     * {@inheritDoc}
     */
    public SetOperation(String idxName, byte[][] keys, byte[][] values) {
        super(idxName, keys, values);
    }

    public SetOperation(String idxName, List<byte[]> keys, List<byte[]> values) {
        super(idxName, keys, values);
    }

    @Override
    public Serializable execute(ILocalDirectory directory) throws GridException {
        final String idxName = getName();
        final byte[][] keys = getKeys();
        final byte[][] values = getValues();
        try {
            directory.setMapping(idxName, keys, values);
        } catch (IndexException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
        return null;
    }

}

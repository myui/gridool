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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class MultiKeyOperation implements DHTOperation {

    protected String[] keys;

    public MultiKeyOperation(String... keys) {
        this.keys = keys;
    }

    public String[] getKeys() {
        return keys;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        final String[] ary = new String[len];
        for(int i = 0; i < len; i++) {
            ary[i] = IOUtils.readString(in);
        }
        this.keys = ary;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(keys.length);
        for(String k : keys) {
            IOUtils.writeString(out, k);
        }
    }

    public abstract MultiKeyOperation makeOperation(String[] keys);

}

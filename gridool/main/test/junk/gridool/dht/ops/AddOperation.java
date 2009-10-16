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
import java.io.Serializable;

import gridool.dht.DHTServer;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class AddOperation extends MultiKeyOperation {

    private Serializable value;

    public AddOperation() {}

    public AddOperation(String[] keys, Serializable value) {
        super(keys);
        this.value = value;
    }

    @Override
    public DHTOps getOperationType() {
        return DHTOps.add;
    }

    @Override
    public boolean isAsyncOps() {
        return true;
    }

    @Override
    public Serializable execute(DHTServer server) {
        server.add(keys, value);
        return null;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.value = (Serializable) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(value);
    }

    @Override
    public AddOperation makeOperation(String[] keys) {
        return new AddOperation(keys, value);
    }

}

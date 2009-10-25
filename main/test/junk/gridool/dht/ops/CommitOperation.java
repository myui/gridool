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
import gridool.dht.construct.ValueWithVersion;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CommitOperation extends KeyOperation {

    private ValueWithVersion value;
    private boolean writeOps;

    public CommitOperation(String key, ValueWithVersion value, boolean writeOps) {
        super(key);
    }

    @Override
    public DHTOps getOperationType() {
        return DHTOps.commit;
    }

    @Override
    public boolean isAsyncOps() {
        return true;
    }

    @Override
    public Serializable execute(DHTServer server) {
        server.commitOperation(key, value, writeOps);
        return null;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.value = ValueWithVersion.readObject(in);
        this.writeOps = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        value.writeExternal(out);
        out.writeBoolean(writeOps);
    }

}

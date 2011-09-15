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
package gridool.dht;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;

import gridool.Grid;
import gridool.dht.job.DHTAddJob;
import gridool.dht.ops.AddOperation;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTClient implements DHT {

    private final Grid grid;

    public DHTClient(Grid grid) {
        this.grid = grid;
    }

    @Override
    public <T extends Serializable> void add(String[] keys, T value) throws DHTException {
        if(keys.length == 0) {
            return;
        }
        final AddOperation ops = new AddOperation(keys, value);
        try {
            grid.execute(DHTAddJob.class, ops);
        } catch (RemoteException e) {
            throw new DHTException(e);
        }
    }

    // --------------------------------------

    @Override
    public boolean flush() throws IOException {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public void close() {
    // nop
    }

}

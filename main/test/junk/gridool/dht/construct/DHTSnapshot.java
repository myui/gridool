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
package gridool.dht.construct;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTSnapshot implements Externalizable {

    private List<DHTEntrySnapshot> entries;

    public DHTSnapshot() {}// for Externalizable

    public DHTSnapshot(List<DHTEntrySnapshot> entries) {
        this.entries = entries;
    }

    public List<DHTEntrySnapshot> getSnapshotEntries() {
        return entries;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        final List<DHTEntrySnapshot> list = new ArrayList<DHTEntrySnapshot>(size);
        for(int i = 0; i < size; i++) {
            DHTEntrySnapshot e = DHTEntrySnapshot.readObject(in);
            list.add(e);
        }
        this.entries = list;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(entries.size());
        for(DHTEntrySnapshot e : entries) {
            e.writeExternal(out);
        }
    }

}

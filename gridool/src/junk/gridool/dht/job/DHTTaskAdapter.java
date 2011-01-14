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
package gridool.dht.job;

import java.io.Serializable;

import gridool.GridException;
import gridool.GridJob;
import gridool.annotation.DHTServerResource;
import gridool.construct.GridTaskAdapter;
import gridool.dht.DHTServer;
import gridool.dht.ops.DHTOperation;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTTaskAdapter extends GridTaskAdapter {
    private static final long serialVersionUID = -330425538181019736L;

    private final DHTOperation ops;

    @DHTServerResource
    private DHTServer dhtServer;

    @SuppressWarnings("unchecked")
    public DHTTaskAdapter(GridJob job, DHTOperation ops) {
        super(job);
        this.ops = ops;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public boolean isAsyncTask() {
        return ops.isAsyncOps();
    }

    @Override
    public Serializable execute() throws GridException {
        if(dhtServer == null) {
            throw new IllegalStateException("DHTServer is not injected");
        }
        return ops.execute(dhtServer);
    }

}

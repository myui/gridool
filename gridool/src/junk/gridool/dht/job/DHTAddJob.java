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
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTAddJob extends MultiKeyDHTJob<Serializable> {
    private static final long serialVersionUID = 6030232480770304656L;

    public DHTAddJob() {
        super();
    }

    @Override
    public boolean isAsyncOps() {
        return true;
    }

    @Override
    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        throw new IllegalStateException("Response to an asynchronous job is illegally accepted.");
    }

    @Override
    public Serializable reduce() throws GridException {
        throw new IllegalStateException("Response to an asynchronous job is illegally accepted.");
    }

}

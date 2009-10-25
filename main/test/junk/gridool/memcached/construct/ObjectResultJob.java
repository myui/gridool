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
package gridool.memcached.construct;

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
public final class ObjectResultJob extends MemcachedJob<Serializable> {
    private static final long serialVersionUID = 679946610565114227L;

    private Serializable firstResult = null;

    public ObjectResultJob() {
        super();
    }

    @Override
    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        if(firstResult == null) {
            final Serializable res = result.getResult();
            if(res != null) {
                this.firstResult = res;
                if(isReadOnlyOps()) {
                    return GridTaskResultPolicy.RETURN;
                }
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    @Override
    public Serializable reduce() throws GridException {
        return firstResult;
    }

}

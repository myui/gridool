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
import gridool.GridJob;
import gridool.construct.GridTaskAdapter;
import gridool.memcached.ops.MemcachedOperation;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class MemcachedTaskAdapter extends GridTaskAdapter {
    private static final long serialVersionUID = 112552715735713051L;

    private final MemcachedOperation ops;

    @SuppressWarnings("unchecked")
    public MemcachedTaskAdapter(GridJob job, MemcachedOperation ops) {
        super(job);
        assert (ops != null);
        this.ops = ops;
    }

    public MemcachedOperation getOperation() {
        return ops;
    }

    @Override
    public Serializable execute() throws GridException {
        throw new IllegalStateException("MemcachedTaskAdapter#execute should not be called.");
    }

}

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
package gridool.memcached.ops;

import gridool.memcached.construct.MemcachedOps;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class FlushOperation implements MemcachedOperation {
    private static final long serialVersionUID = 3031764087870808800L;

    private final boolean async;

    public FlushOperation(boolean async) {
        this.async = async;
    }

    @Override
    public boolean isAsyncOps() {
        return async;
    }

    @Override
    public boolean isReadOnlyOps() {
        return false;
    }

    @Override
    public MemcachedOps getOperationType() {
        return MemcachedOps.flush;
    }

    @Override
    public int getNumberOfReplicas() {
        return 0;
    }

    @Override
    public void setNumberOfReplicas(int replicas) {}

    @Override
    public String getKey() {
        return null;
    }
}

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

import java.io.Serializable;

import gridool.memcached.construct.MemcachedOps;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class AppendOperation<T extends Serializable> extends KeyValueOperation<T> {
    private static final long serialVersionUID = 1743877704485257143L;

    public AppendOperation(String key, T value, boolean async) {
        super(key, value, async);
    }

    @Override
    public MemcachedOps getOperationType() {
        return MemcachedOps.append;
    }

}

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

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class KeyOperation implements MemcachedOperation {
    private static final long serialVersionUID = 1896208176043998127L;

    private final String key;
    private final boolean async;
    private int replicas = 0;

    protected KeyOperation(String key, boolean async) {
        this.key = key;
        this.async = async;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public boolean isAsyncOps() {
        return async;
    }

    @Override
    public boolean isReadOnlyOps() {
        return true;
    }

    @Override
    public int getNumberOfReplicas() {
        return replicas;
    }

    @Override
    public void setNumberOfReplicas(int replicas) {
        this.replicas = replicas;
    }

}

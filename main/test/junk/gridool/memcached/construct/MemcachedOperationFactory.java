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

import gridool.memcached.ops.AddOperation;
import gridool.memcached.ops.AppendOperation;
import gridool.memcached.ops.CASOperation;
import gridool.memcached.ops.DeleteOperation;
import gridool.memcached.ops.FlushOperation;
import gridool.memcached.ops.GetAllOperation;
import gridool.memcached.ops.GetOperation;
import gridool.memcached.ops.GetsOperation;
import gridool.memcached.ops.KeyValueOperation;
import gridool.memcached.ops.PrepareOperation;
import gridool.memcached.ops.PrependOperation;
import gridool.memcached.ops.ReplaceOperation;
import gridool.memcached.ops.SetOperation;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MemcachedOperationFactory {

    private MemcachedOperationFactory() {}

    public static <T extends Serializable> KeyValueOperation<T> createSetOperation(String key, T value, boolean async) {
        return new SetOperation<T>(key, value, async);
    }

    public static <T extends Serializable> AddOperation<T> createAddOperation(String key, T value, boolean async) {
        return new AddOperation<T>(key, value, async);
    }

    public static <T extends Serializable> ReplaceOperation<T> createReplaceOperation(String key, T value) {
        return new ReplaceOperation<T>(key, value);
    }

    public static <T extends Serializable> CASOperation<T> createCASOperation(String key, long casId, T value, boolean async) {
        return new CASOperation<T>(key, casId, value, async);
    }

    public static <T extends Serializable> AppendOperation<T> createAppendOperation(String key, T value, boolean async) {
        return new AppendOperation<T>(key, value, async);
    }

    public static <T extends Serializable> PrependOperation<T> createPrependOperation(String key, T value, boolean async) {
        return new PrependOperation<T>(key, value, async);
    }

    public static GetOperation createGetOperation(String key) {
        return new GetOperation(key);
    }

    public static GetAllOperation createGetAllOperation(String[] keys) {
        return new GetAllOperation(keys);
    }

    public static GetsOperation createGetsOperation(String key) {
        return new GetsOperation(key);
    }

    public static DeleteOperation createDeleteOperation(String key, boolean async) {
        return new DeleteOperation(key, async);
    }

    public static FlushOperation createFlushOperation(boolean async) {
        return new FlushOperation(async);
    }

    public static PrepareOperation createPrepareOperation(String key, boolean writeOps) {
        return new PrepareOperation(key, writeOps);
    }

}

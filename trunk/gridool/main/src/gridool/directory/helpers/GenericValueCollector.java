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
package gridool.directory.helpers;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;

import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.util.converter.DataConverter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GenericValueCollector<T> implements BTreeCallback {

    private final List<T> result;
    private final DataConverter<T> convertor;

    public GenericValueCollector(@CheckForNull DataConverter<T> converter) {
        if(converter == null) {
            throw new IllegalArgumentException();
        }
        this.convertor = converter;
        this.result = new ArrayList<T>(32);
    }

    public List<T> getResult() {
        return result;
    }

    public void clear() {
        result.clear();
    }

    public boolean indexInfo(Value key, byte[] value) {
        T converted = convertor.decode(value);
        result.add(converted);
        return true;
    }

    public boolean indexInfo(Value value, long pointer) {
        throw new IllegalStateException();
    }
}

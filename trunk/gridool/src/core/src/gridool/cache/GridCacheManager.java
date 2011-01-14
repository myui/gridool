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
package gridool.cache;

import gridool.util.collections.ObservableLRUMap;

import java.util.HashMap;
import java.util.Map;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridCacheManager {

    private final Map<String, ObservableLRUMap<?, ?>> caches;

    public GridCacheManager() {
        this.caches = new HashMap<String, ObservableLRUMap<?, ?>>(128);
    }

    @SuppressWarnings("unchecked")
    public <K, V> ObservableLRUMap<K, V> buildCache(String name, int maxCapacity) {
        ObservableLRUMap<?, ?> value;
        synchronized(caches) {
            value = caches.get(name);
            if(value == null) {
                value = new ObservableLRUMap<K, V>(maxCapacity);
                caches.put(name, value);
            }
        }
        return (ObservableLRUMap<K, V>) value;
    }

    public void removeCache(String name) {
        synchronized(caches) {
            caches.remove(name);
        }
    }

    public void clearCache(String name) {
        synchronized(caches) {
            Map<?, ?> value = caches.get(name);
            if(value != null) {
                value.clear();
            }
        }
    }
}

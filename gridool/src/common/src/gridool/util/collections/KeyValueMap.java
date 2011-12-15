/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.util.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class KeyValueMap<K, V> implements Map<K, V>, Serializable {
    private static final long serialVersionUID = -8572713274583101985L;

    protected final List<K> keys;
    protected final List<V> values;

    public KeyValueMap() {
        this(32);
    }

    public KeyValueMap(int size) {
        this.keys = new ArrayList<K>(size);
        this.values = new ArrayList<V>(size);
    }

    public KeyValueMap(List<K> k, List<V> v) {
        this.keys = k;
        this.values = v;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        keys.clear();
        values.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return keys.contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return values.contains(value);
    }

    @Override
    public V get(Object key) {
        int index = keys.indexOf(key);
        if(index == -1) {
            return null;
        }
        return values.get(index);
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(K key, V value) {
        keys.add(key);
        values.add(value);
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for(Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            keys.add(e.getKey());
            values.add(e.getValue());
        }
    }

    @Override
    public V remove(Object key) {
        int index = keys.indexOf(key);
        if(index == -1) {
            return null;
        }
        keys.remove(index);
        V old = values.remove(index);
        return old;
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public Collection<V> values() {
        return values;
    }

    @Override
    public String toString() {
        return "KeyValueList [keys=" + keys + ", values=" + values + "]";
    }

}

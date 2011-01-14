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
package gridool.util.collections;

import gridool.util.lang.ArrayUtils;

import java.io.Serializable;
import java.lang.reflect.Array;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class ArrayQueue<T> implements Serializable {
    private static final long serialVersionUID = 7432315428377829289L;

    public static final int DEFAULT_ARY_SIZE = 16;

    protected/* transient */int _pos = 0; // TODO serialization
    protected int _lastIndex = 0;

    protected int _arraySize;
    protected Object[] _array;

    public ArrayQueue() {
        this(DEFAULT_ARY_SIZE);
    }

    public ArrayQueue(int arysize) {
        this._array = new Object[arysize];
        this._arraySize = arysize;
    }

    public ArrayQueue(T[] array) {
        this._array = array;
        this._arraySize = array.length;
    }

    public ArrayQueue(T[] array, int cur, int last) {
        this._array = array;
        this._arraySize = array.length;
        this._pos = cur;
        this._lastIndex = last;
    }

    public boolean offer(T x) {
        if(isFull()) {
            growArray();
        }
        _array[_lastIndex++] = x;
        return false;
    }

    protected final boolean isFull() {
        return _lastIndex >= _arraySize;
    }

    @SuppressWarnings("unchecked")
    public final T poll() {
        if(_pos < _lastIndex) {
            T obj = (T) _array[_pos++];
            if(_pos == _lastIndex) {
                clear();
            }
            return obj;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public final T peek() {
        return (_pos < _lastIndex) ? (T) _array[_pos] : null;
    }

    @SuppressWarnings("unchecked")
    public final T get(int index) {
        final int pos = _pos + index;
        if(pos >= _lastIndex) {
            throw new IndexOutOfBoundsException("Index: " + index + ", pos: " + _pos + ", Size: "
                    + _lastIndex);
        }
        return (T) _array[pos];
    }

    @SuppressWarnings("unchecked")
    public final T unsafeGet(int index) {
        return (T) _array[_pos + index];
    }

    public final boolean isEmpty() {
        return _pos >= _lastIndex;
    }

    public final void clear() {
        _pos = 0;
        _lastIndex = 0;
    }

    public final int size() {
        return _lastIndex - _pos;
    }

    private void growArray() {
        Object[] newArray = new Object[_arraySize * 2];
        System.arraycopy(_array, 0, newArray, 0, _arraySize);
        _array = newArray;
        _arraySize = newArray.length;
    }

    public final Object[] toArray() {
        if(_arraySize == 0) {
            return new Object[0];
        }
        final int size = size();
        final Object[] ary = new Object[size];
        System.arraycopy(_array, _pos, ary, 0, size);
        return ary;
    }

    @SuppressWarnings("unchecked")
    public final T[] toArray(Class<T> componentType) {
        final int size = size();
        final T[] copy = (T[]) Array.newInstance(componentType, size);
        System.arraycopy(_array, 0, copy, 0, size);
        return copy;
    }

    @SuppressWarnings("unchecked")
    public final T[] toArray(T[] a) {
        final int size = size();
        if(a.length < size) {
            return (T[]) ArrayUtils.copyOf(_array, size, a.getClass());
        }
        System.arraycopy(_array, 0, a, 0, size);
        return a;
    }

}

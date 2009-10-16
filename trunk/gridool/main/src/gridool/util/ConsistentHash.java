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
package gridool.util;

import gridool.GridLocatable;
import gridool.GridNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import xbird.util.lang.ArrayUtils;
import xbird.util.lang.Primitives;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@NotThreadSafe
public final class ConsistentHash {

    private final HashFunction hashFunction;
    private final int numberOfVirtualNodes;

    private final SortedMap<Long, GridNode> circle;
    private final Set<GridNode> nodes;

    public ConsistentHash(@Nonnull HashFunction hashFunction, @Nonnegative int numberOfVirtualNodes) {
        assert (numberOfVirtualNodes > 0);
        this.hashFunction = hashFunction;
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.circle = new TreeMap<Long, GridNode>();
        this.nodes = new HashSet<GridNode>();
    }

    @Deprecated
    public ConsistentHash(@Nonnull HashFunction hashFunction, @Nonnegative int numberOfVirtualNodes, @Nonnull Collection<GridNode> nodes) {
        assert (numberOfVirtualNodes > 0);
        this.hashFunction = hashFunction;
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.circle = new TreeMap<Long, GridNode>();
        this.nodes = new HashSet<GridNode>();
        for(GridNode node : nodes) {
            add(node);
        }
    }

    public int getNumberOfVirtualNodes() {
        return numberOfVirtualNodes;
    }

    public GridNode[] getNodes() {
        final GridNode[] ary = new GridNode[nodes.size()];
        return nodes.toArray(ary);
    }

    public void add(@Nonnull GridNode node) {
        final String key = node.getKey();
        if(circle.put(hashFunction.hash(key), node) == null) {
            nodes.add(node);
        }
        for(int i = 1; i < numberOfVirtualNodes; i++) {
            long k = hashFunction.hash(key + i);
            circle.put(k, node);
        }
    }

    public void remove(@Nonnull GridNode node) {
        final String key = node.getKey();
        circle.remove(hashFunction.hash(key));
        for(int i = 1; i < numberOfVirtualNodes; i++) {
            long k = hashFunction.hash(key + i);
            circle.remove(k);
        }
    }

    public GridNode get(@Nonnull GridLocatable key) {
        return get(key.getKey());
    }

    public GridNode get(@Nonnull String key) {
        if(circle.isEmpty()) {
            return null;
        }
        Long hash = hashFunction.hash(key);
        if(!circle.containsKey(hash)) {
            SortedMap<Long, GridNode> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private GridNode getInternal(long hashKey) {
        if(!circle.containsKey(hashKey)) {
            SortedMap<Long, GridNode> tailMap = circle.tailMap(hashKey);
            hashKey = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hashKey);
    }

    public Iterator<GridNode> getAll(@Nonnull GridLocatable key) {
        String s = key.getKey();
        byte[] b = StringUtils.getBytes(s);
        return getAll(b, numberOfVirtualNodes);
    }

    public Iterator<GridNode> getAll(@Nonnull byte[] key) {
        return getAll(key, numberOfVirtualNodes);
    }

    public Iterator<GridNode> getAll(@Nonnull byte[] key, int numOfReplicas) {
        return new Itor(key, numOfReplicas);
    }

    public Collection<GridNode> getAll() {
        return circle.values();
    }

    public void clear() {
        circle.clear();
    }

    private final class Itor implements Iterator<GridNode> {

        final byte[] key;
        final byte[] tmpKey;
        final int numTries;

        long hashval;
        int tries = 1;

        public Itor(@Nonnull byte[] key, @Nonnegative int numberOfReplicas) {
            assert (numberOfReplicas > 0);
            this.key = key;
            this.tmpKey = ArrayUtils.copyOf(key, key.length + 4);
            this.numTries = numberOfReplicas;
            this.hashval = hashFunction.hash(key);
        }

        public boolean hasNext() {
            return tries <= numTries;
        }

        public GridNode next() {
            assert (hasNext());
            long hashval = advance();
            return getInternal(hashval);
        }

        private long advance() {
            long hash = hashval;
            final byte[] k = tmpKey;
            Primitives.putInt(k, key.length, tries);
            this.hashval = hashFunction.hash(k);
            tries++;
            return hash;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}

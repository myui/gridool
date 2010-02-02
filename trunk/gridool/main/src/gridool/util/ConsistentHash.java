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

import gridool.GridNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@NotThreadSafe
public final class ConsistentHash {
    private static final Log LOG = LogFactory.getLog(ConsistentHash.class);

    private final HashFunction hashFunction;
    private final int numberOfVirtualNodes;

    private final SortedMap<Long, GridNode> circle;
    private final Set<GridNode> nodes;

    public ConsistentHash(@Nonnull HashFunction hashFunction, @Nonnegative int numberOfVirtualNodes) {
        assert (numberOfVirtualNodes > 0) : numberOfVirtualNodes;
        this.hashFunction = hashFunction;
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.circle = new TreeMap<Long, GridNode>();
        this.nodes = new HashSet<GridNode>();
    }

    public int getNumberOfVirtualNodes() {
        return numberOfVirtualNodes;
    }

    public void add(@Nonnull GridNode node) {
        if(nodes.add(node)) {
            final String key = node.getKey();
            if(circle.put(hashFunction.hash(key), node) != null) {
                throw new IllegalStateException();
            }
            for(int i = 1; i < numberOfVirtualNodes; i++) {
                long k = hashFunction.hash(key + i);
                circle.put(k, node);
            }
        }
    }

    public void remove(@Nonnull GridNode node) {
        if(nodes.remove(node)) {
            final String key = node.getKey();
            if(circle.remove(hashFunction.hash(key)) == null) {
                throw new IllegalStateException();
            }
            for(int i = 1; i < numberOfVirtualNodes; i++) {
                long k = hashFunction.hash(key + i);
                circle.remove(k);
            }
        }
    }

    public GridNode get(@Nonnull byte[] key) {
        if(circle.isEmpty()) {
            return null;
        }
        Long hash = hashFunction.hash(key);
        GridNode node = circle.get(hash);
        if(node != null) {
            return node;
        }
        SortedMap<Long, GridNode> tailMap = circle.tailMap(hash);
        hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        return circle.get(hash);
    }

    public List<GridNode> listSuccessors(@Nonnull byte[] fromKey, int numToGets, boolean exclusive) {
        if(circle.isEmpty()) {
            return Collections.emptyList();
        }
        numToGets = Math.min(numToGets, circle.size());
        final List<GridNode> retNodes = new ArrayList<GridNode>(numToGets);
        int accquired = 0;

        final Long hash = hashFunction.hash(fromKey);

        if(exclusive && LOG.isWarnEnabled()) {
            if(!circle.containsKey(hash)) {
                LOG.warn("Routing table does not have a required node");
            }
        }

        final SortedMap<Long, GridNode> tailMap = circle.tailMap(hash);
        final boolean noTail = tailMap.isEmpty();
        if(!noTail) {
            final Iterator<GridNode> tail = tailMap.values().iterator();
            if(exclusive) {
                assert (tail.hasNext());
                tail.next(); // skip
            }
            for(; tail.hasNext() && accquired < numToGets; accquired++) {
                GridNode n = tail.next();
                if(!retNodes.contains(n)) {
                    retNodes.add(n);
                }
            }
        }
        if(accquired < numToGets) {
            final Iterator<GridNode> head = circle.values().iterator();
            if(exclusive && noTail) {
                assert (head.hasNext());
                head.next(); // skip
            }
            for(; head.hasNext() && accquired < numToGets; accquired++) {
                GridNode n = head.next();
                if(!retNodes.contains(n)) {
                    retNodes.add(n);
                }
            }
        }
        return retNodes;
    }

    public GridNode[] getAll() {
        final GridNode[] ary = new GridNode[nodes.size()];
        return nodes.toArray(ary);
    }

    public void clear() {
        circle.clear();
        nodes.clear();
    }
}

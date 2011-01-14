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
package gridool.routing.strategy;

import gridool.GridNode;
import gridool.util.hashes.HashFunction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public final class ConsistentHash implements Externalizable {
    private static final Log LOG = LogFactory.getLog(ConsistentHash.class);

    @Nonnull
    private/* final */HashFunction hashFunction;
    private/* final */int numberOfVirtualNodes;

    @Nonnull
    private/* final */SortedMap<Long, GridNode> circle;
    @Nonnull
    private/* final */SortedMap<GridNode, GridNode> liveNodes;
    @Nonnull
    private/* final */Map<GridNode, GridNode> removedNodes;

    public ConsistentHash() {}// Externalizable

    public ConsistentHash(@Nonnull HashFunction hashFunction, @Nonnegative int numberOfVirtualNodes) {
        assert (numberOfVirtualNodes > 0) : numberOfVirtualNodes;
        this.hashFunction = hashFunction;
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.circle = new TreeMap<Long, GridNode>();
        this.liveNodes = new TreeMap<GridNode, GridNode>(new NodeComparator(hashFunction));
        this.removedNodes = new HashMap<GridNode, GridNode>(32);
    }

    private static final class NodeComparator implements Comparator<GridNode> {

        private final HashFunction hashFunction;

        public NodeComparator(HashFunction hashFunction) {
            this.hashFunction = hashFunction;
        }

        public int compare(GridNode o1, GridNode o2) {
            final long l1 = hashFunction.hash(o1.getKey());
            final long l2 = hashFunction.hash(o2.getKey());
            if(l1 > l2) {
                return 1;
            } else if(l1 < l2) {
                return -1;
            }
            return o1.compareTo(o2);
        }

    }

    public int getNumberOfVirtualNodes() {
        return numberOfVirtualNodes;
    }

    public void add(@Nonnull GridNode node) {
        if(!liveNodes.containsKey(node)) {
            if(liveNodes.put(node, node) != null) {
                throw new IllegalStateException();
            }
            final String key = node.getKey();
            if(circle.put(hashFunction.hash(key), node) != null) {
                throw new IllegalStateException();
            }
            for(int i = 1; i < numberOfVirtualNodes; i++) {
                long k = hashFunction.hash(key + i);
                circle.put(k, node);
            }
            removedNodes.remove(node);
        }
    }

    public void remove(@Nonnull GridNode node) {
        GridNode removed = liveNodes.remove(node);
        if(removed != null) {
            final String key = node.getKey();
            if(circle.remove(hashFunction.hash(key)) == null) {
                throw new IllegalStateException();
            }
            for(int i = 1; i < numberOfVirtualNodes; i++) {
                long k = hashFunction.hash(key + i);
                circle.remove(k);
            }
            removedNodes.put(removed, removed);
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

    @Nullable
    public GridNode resolveNode(@Nonnull GridNode targetNode) {
        GridNode node = liveNodes.get(targetNode);
        if(node == null) {
            node = removedNodes.get(targetNode);
        }
        return node;
    }

    public List<GridNode> listSuccessorsInVirtualChain(@Nonnull final byte[] fromKey, final int maxNodesToSelect, final boolean exclusive) {
        if(circle.isEmpty()) {
            return Collections.emptyList();
        }
        int numToGets = Math.min(maxNodesToSelect, liveNodes.size());
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
            for(; tail.hasNext() && accquired < numToGets;) {
                GridNode n = tail.next();
                if(!retNodes.contains(n)) {
                    retNodes.add(n);
                    accquired++;
                }
            }
        }
        if(accquired < numToGets) {
            final Iterator<GridNode> head = circle.values().iterator();
            if(exclusive && noTail) {
                assert (head.hasNext());
                head.next(); // skip
            }
            for(; head.hasNext() && accquired < numToGets;) {
                GridNode n = head.next();
                if(!retNodes.contains(n)) {
                    retNodes.add(n);
                    accquired++;
                }
            }
        }
        return retNodes;
    }

    public List<GridNode> listSuccessorsInPhysicalChain(@Nonnull final GridNode node, final int maxNodesToSelect, final boolean exclusive) {
        int numToGets = Math.min(maxNodesToSelect, liveNodes.size());
        final List<GridNode> retNodes = new ArrayList<GridNode>(numToGets);
        int accquired = 0;

        final SortedMap<GridNode, GridNode> tailMap = liveNodes.tailMap(node);
        final boolean noTail = tailMap.isEmpty();
        if(!noTail) {// has tail
            final Iterator<GridNode> tail = tailMap.values().iterator();
            if(exclusive) {
                assert (tail.hasNext());
                tail.next(); // skip
            }
            for(; tail.hasNext() && accquired < numToGets; accquired++) {
                GridNode n = tail.next();
                retNodes.add(n);
            }
        }
        if(accquired < numToGets) {
            final Iterator<GridNode> head = liveNodes.values().iterator();
            if(exclusive && noTail) {
                assert (head.hasNext());
                head.next(); // skip
            }
            for(; head.hasNext() && accquired < numToGets; accquired++) {
                GridNode n = head.next();
                retNodes.add(n);
            }
        }
        return retNodes;
    }

    public GridNode[] getAll() {
        final GridNode[] ary = new GridNode[liveNodes.size()];
        return liveNodes.values().toArray(ary);
    }

    public void clear() {
        circle.clear();
        liveNodes.clear();
        removedNodes.clear();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        HashFunction hashFunc = (HashFunction) in.readObject();
        this.hashFunction = hashFunc;
        this.numberOfVirtualNodes = in.readInt();
        this.circle = new TreeMap<Long, GridNode>();
        this.liveNodes = new TreeMap<GridNode, GridNode>(new NodeComparator(hashFunc));
        this.removedNodes = new HashMap<GridNode, GridNode>(32);
        final int numNodes = in.readInt();
        for(int i = 0; i < numNodes; i++) {
            GridNode node = (GridNode) in.readObject();
            add(node);
        }
        final int numRemoved = in.readInt();
        final Map<GridNode, GridNode> removedNodes = new HashMap<GridNode, GridNode>(numRemoved * 2);
        for(int i = 0; i < numRemoved; i++) {
            GridNode node = (GridNode) in.readObject();
            removedNodes.put(node, node);
        }
        this.removedNodes = removedNodes;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(hashFunction);
        out.writeInt(numberOfVirtualNodes);
        final GridNode[] nodes = getAll();
        out.writeInt(nodes.length);
        for(GridNode node : nodes) {
            out.writeObject(node);
        }
        int numRemoved = removedNodes.size();
        out.writeInt(numRemoved);
        for(GridNode node : removedNodes.values()) {
            out.writeObject(node);
        }
    }

}

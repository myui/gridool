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
package gridool.db.partitioning.phihash;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.util.primitive.Primitives;

import javax.annotation.Nonnull;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class NodeWithPartitionNo {

    private final GridNode node;
    private final int partitionNo;

    public NodeWithPartitionNo(@Nonnull GridNode node, int partitionNo) {
        assert (node != null);
        this.node = node;
        this.partitionNo = partitionNo;
    }

    public GridNode getNode() {
        return node;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + node.hashCode();
        result = prime * result + partitionNo;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj == null) {
            return false;
        }
        if(getClass() != obj.getClass()) {
            return false;
        }
        final NodeWithPartitionNo other = (NodeWithPartitionNo) obj;
        if(partitionNo != other.partitionNo) {
            return false;
        }
        if(!node.equals(other.node)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return node.toString() + " [partitionNo=" + partitionNo + "]";
    }

    public byte[] serialize() {
        return serialize(node, partitionNo);
    }
    
    public static byte[] serialize(final GridNode node, final int partitionNo) {
        if(partitionNo < 1 || partitionNo > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Illeal PartitionNo: " + partitionNo);
        }
        byte[] nodeBytes = node.toBytes();
        final byte[] b = new byte[nodeBytes.length + 2];
        Primitives.putShort(b, 0, (short) partitionNo);
        System.arraycopy(nodeBytes, 0, b, 2, nodeBytes.length);
        return b;
    }

    public static NodeWithPartitionNo deserialize(final byte[] b) {
        int partitionNo = Primitives.getShortAsInt(b, 0);
        int nodeLength = b.length - 2;
        final byte[] nodeBytes = new byte[nodeLength];
        System.arraycopy(b, 2, nodeBytes, 0, nodeLength);
        GridNode node = GridNodeInfo.fromBytes(nodeBytes);
        return new NodeWithPartitionNo(node, partitionNo);
    }

    public static int deserializePartitionNo(final byte[] b) {
        final int partitionNo = Primitives.getShortAsInt(b, 0);
        if(partitionNo < 1 || partitionNo > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Illeal PartitionNo: " + partitionNo);
        }
        return partitionNo;
    }

    public static GridNode deserializeGridNode(final byte[] b) {
        int nodeLength = b.length - 2;
        assert (nodeLength > 0) : nodeLength;
        final byte[] nodeBytes = new byte[nodeLength];
        System.arraycopy(b, 2, nodeBytes, 0, nodeLength);
        GridNode node = GridNodeInfo.fromBytes(nodeBytes);
        return node;
    }
}
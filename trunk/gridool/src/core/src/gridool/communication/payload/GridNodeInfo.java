/*
 * @(#)$Id$
 *
 * Copyright 2006-2010 Makoto YUI
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
package gridool.communication.payload;

import gridool.GridLocatable;
import gridool.GridNode;
import gridool.GridNodeMetrics;
import gridool.util.GridUtils;
import gridool.util.io.IOUtils;
import gridool.util.net.NetUtils;
import gridool.util.primitive.Primitives;
import gridool.util.string.StringUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GridNodeInfo implements GridNode, Externalizable {
    private static final long serialVersionUID = -4441612143966938941L;

    @Nonnull
    private/* final */InetAddress addr;
    @Nonnull
    private/* final */byte[] hardName;
    private/* final */int port;
    @Nonnull
    private/* final */String idenfider;
    private/* final */boolean superNode;
    @Nonnull
    private List<GridNode> replicas;

    // -----------------------------------------------------------

    @Nullable
    private transient GridNodeMetrics metrics;

    // -----------------------------------------------------------

    public GridNodeInfo() {}//for Externalizable

    public GridNodeInfo(int port, boolean superNode) {
        this(NetUtils.getLocalHost(), port, superNode);
    }

    public GridNodeInfo(@CheckForNull InetAddress addr, int port, boolean superNode) {
        if(addr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        String fqdn = NetUtils.getFQDN(addr);
        this.hardName = StringUtils.getBytes(fqdn);
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(hardName, port);
        this.superNode = superNode;
        this.replicas = new ArrayList<GridNode>(4);
    }

    public GridNodeInfo(@CheckForNull InetAddress addr, int port, @CheckForNull byte[] hardName, boolean superNode) {
        if(addr == null) {
            throw new IllegalArgumentException();
        }
        if(hardName == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.hardName = hardName;
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(hardName, port);
        this.superNode = superNode;
        this.replicas = new ArrayList<GridNode>(4);
    }

    protected GridNodeInfo(@CheckForNull GridNode copyNode, @Nullable GridNodeMetrics metrics) {
        if(copyNode == null) {
            throw new IllegalArgumentException();
        }
        this.addr = copyNode.getPhysicalAdress();
        this.hardName = copyNode.getHardName();
        assert (hardName != null);
        this.port = copyNode.getPort();
        this.superNode = copyNode.isSuperNode();
        this.idenfider = copyNode.getKey();
        this.replicas = new ArrayList<GridNode>(4);
        this.metrics = metrics;
    }

    public final InetAddress getPhysicalAdress() {
        return addr;
    }

    public final byte[] getHardName() {
        return hardName;
    }

    public final int getPort() {
        return port;
    }

    public final String getKey() {
        return idenfider;
    }

    public final boolean isSuperNode() {
        return superNode;
    }

    public List<GridNode> getReplicas() {
        return replicas;
    }

    public void setReplicas(@Nonnull List<GridNode> replicas) {
        this.replicas = replicas;
    }

    public final GridNodeMetrics getMetrics() {
        return metrics;
    }

    public final void setMetrics(@Nonnull GridNodeMetrics metrics) {
        if(metrics == null) {
            throw new IllegalArgumentException();
        }
        this.metrics = metrics;
    }

    // -----------------------------------------------------------

    @Override
    public final int hashCode() {
        return idenfider.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(obj instanceof GridNode) {
            GridNode otherNode = (GridNode) obj;
            return equals(otherNode);
        }
        return false;
    }

    public final boolean equals(GridNode otherNode) {
        String otherId = otherNode.getKey();
        return idenfider.equals(otherId);
    }

    public final int compareTo(GridLocatable other) {
        if(this == other) {
            return 0;
        }
        if(!(other instanceof GridNode)) {
            throw new IllegalArgumentException(getClass().getName() + " is uncomparable to class: "
                    + other.getClass().getName());
        }
        String otherId = ((GridNode) other).getKey();
        return idenfider.compareTo(otherId);
    }

    @Override
    public String toString() {
        return GridUtils.toNodeInfo(this);
    }

    // -----------------------------------------------------------

    /**
     * 1 bytes - ip address length
     * 1 bytes - hard name length (K)
     * 1 byte  - is super node
     * 4 bytes - port number
     * N bytes - ip addr (N=4 oe N=16)
     * K bytes - hard name
     */
    @Nonnull
    public byte[] toBytes() {
        byte[] ipAddr = addr.getAddress();
        int ipLength = ipAddr.length;
        int hardLength = hardName.length;
        int total = 7 + ipLength + hardLength;
        final byte[] ret = new byte[total];
        ret[0] = (byte) ipLength;
        ret[1] = (byte) hardLength;
        ret[2] = (byte) (superNode ? 1 : 0);
        Primitives.putInt(ret, 3, port);
        System.arraycopy(ipAddr, 0, ret, 7, ipLength);
        System.arraycopy(hardName, 0, ret, 7 + ipLength, hardLength);
        return ret;
    }

    /**
     * 1 bytes - ip address length
     * 1 bytes - hard name length (usually K)
     * 1 byte  - is super node
     * 4 bytes - port number
     * N bytes - ip addr (N=4 oe N=16)
     * K bytes - hard name
     */
    public static final GridNodeInfo fromBytes(final byte[] in) {
        int ipLength = in[0];
        int hardLength = in[1];
        boolean superNode = (in[2] == 1);
        int port = Primitives.getInt(in, 3);
        final byte[] ip = new byte[ipLength];
        System.arraycopy(in, 7, ip, 0, ipLength);
        final InetAddress ipAddr;
        try {
            ipAddr = InetAddress.getByAddress(ip);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        assert (hardLength > 0);
        final byte[] hardName = new byte[hardLength];
        System.arraycopy(in, 7 + ipLength, hardName, 0, hardLength);
        return new GridNodeInfo(ipAddr, port, hardName, superNode);
    }

    public static final GridNodeInfo readFrom(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        final GridNodeInfo info = new GridNodeInfo();
        info.readExternal(in);
        return info;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] b = IOUtils.readBytes(in);
        this.addr = InetAddress.getByAddress(b);
        this.hardName = IOUtils.readBytes(in);
        this.port = in.readInt();
        this.superNode = in.readBoolean();
        this.idenfider = GridUtils.getNodeIdentifier(hardName, port);

        final int numReplicas = in.readInt();
        final List<GridNode> replicas = new ArrayList<GridNode>(numReplicas);
        for(int i = 0; i < numReplicas; i++) {
            byte[] nodeBytes = IOUtils.readBytes(in);
            GridNode node = fromBytes(nodeBytes);
            replicas.add(node);
        }
        this.replicas = replicas;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] b = addr.getAddress();
        IOUtils.writeBytes(b, out);
        IOUtils.writeBytes(hardName, out);
        out.writeInt(port);
        out.writeBoolean(superNode);

        final int numReplicas = replicas.size();
        out.writeInt(numReplicas);
        for(int i = 0; i < numReplicas; i++) {
            GridNode node = replicas.get(i);
            byte[] nodeBytes = node.toBytes();
            IOUtils.writeBytes(nodeBytes, out);
        }
    }

}

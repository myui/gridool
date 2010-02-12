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

import xbird.util.io.IOUtils;
import xbird.util.net.NetUtils;
import xbird.util.primitive.Primitives;

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
    private/* final */byte[] macAddr;
    private/* final */int port;
    @Nonnull
    private/* final */String idenfider;
    private/* final */boolean superNode;

    // -----------------------------------------------------------

    @Nullable
    private transient GridNodeMetrics metrics;
    @Nonnull
    private transient List<GridNode> replicas = new ArrayList<GridNode>(4);

    // -----------------------------------------------------------

    public GridNodeInfo() {}//for Externalizable

    public GridNodeInfo(int port, boolean superNode) {
        this(NetUtils.getLocalHost(), port, superNode, null);
    }

    protected GridNodeInfo(@CheckForNull InetAddress localAddr, int port, boolean superNode, @Nullable GridNodeMetrics metrics) {
        if(localAddr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = localAddr;
        this.macAddr = NetUtils.getMacAddress(localAddr);
        if(macAddr == null) {
            throw new IllegalStateException("Could not get mac address: " + localAddr);
        }
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(macAddr, port);
        this.superNode = superNode;
    }

    public GridNodeInfo(@CheckForNull InetAddress addr, int port, @CheckForNull byte[] macAddr, boolean superNode) {
        if(addr == null) {
            throw new IllegalArgumentException();
        }
        if(macAddr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.macAddr = macAddr;
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(macAddr, port);
        this.superNode = superNode;
    }

    protected GridNodeInfo(@CheckForNull GridNode copyNode, @Nullable GridNodeMetrics metrics) {
        if(copyNode == null) {
            throw new IllegalArgumentException();
        }
        this.addr = copyNode.getPhysicalAdress();
        this.macAddr = copyNode.getMacAdress();
        this.port = copyNode.getPort();
        this.superNode = copyNode.isSuperNode();
        this.idenfider = copyNode.getKey();
        this.metrics = metrics;
    }

    public final InetAddress getPhysicalAdress() {
        return addr;
    }

    public final byte[] getMacAdress() {
        return macAddr;
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
     * 1 bytes - mac address length (usually K=6)
     * 1 byte  - is super node
     * 4 bytes - port number
     * N bytes - ip addr (N=4 oe N=16)
     * K bytes - mac address
     */
    @Nonnull
    public byte[] toBytes() {
        byte[] ipAddr = addr.getAddress();
        int ipLength = ipAddr.length;
        int macLength = macAddr.length;
        int total = 7 + ipLength + macLength;
        final byte[] ret = new byte[total];
        ret[0] = (byte) ipLength;
        ret[1] = (byte) macLength;
        ret[2] = (byte) (superNode ? 1 : 0);
        Primitives.putInt(ret, 3, port);
        System.arraycopy(ipAddr, 0, ret, 7, ipLength);
        System.arraycopy(macAddr, 0, ret, 7 + ipLength, macLength);
        return ret;
    }

    /**
     * 1 bytes - ip address length
     * 1 bytes - mac address length (usually K=6)
     * 1 byte  - is super node
     * 4 bytes - port number
     * N bytes - ip addr (N=4 oe N=16)
     * K bytes - mac address
     */
    public static final GridNodeInfo fromBytes(final byte[] in) {
        int ipLength = in[0];
        int macLength = in[1];
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
        final byte[] macAddr = new byte[macLength];
        System.arraycopy(in, 7 + ipLength, macAddr, 0, macLength);
        return new GridNodeInfo(ipAddr, port, macAddr, superNode);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] b = IOUtils.readBytes(in);
        this.addr = InetAddress.getByAddress(b);
        this.macAddr = IOUtils.readBytes(in);
        this.port = in.readInt();
        this.superNode = in.readBoolean();
        this.idenfider = GridUtils.getNodeIdentifier(macAddr, port);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] b = addr.getAddress();
        IOUtils.writeBytes(b, out);
        IOUtils.writeBytes(macAddr, out);
        out.writeInt(port);
        out.writeBoolean(superNode);
    }

    public static final GridNodeInfo readFrom(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        final GridNodeInfo info = new GridNodeInfo();
        info.readExternal(in);
        return info;
    }

    // -----------------------------------------------------------

    public final GridNodeMetrics getMetrics() {
        return metrics;
    }

    public final void setMetrics(@Nonnull GridNodeMetrics metrics) {
        if(metrics == null) {
            throw new IllegalArgumentException();
        }
        this.metrics = metrics;
    }

    public List<GridNode> getReplicas() {
        return replicas;
    }

    public void setReplicas(@Nonnull List<GridNode> replicas) {
        this.replicas = replicas;
    }
}

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
import xbird.util.string.StringUtils;

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
    private InetAddress addr;
    private int port;
    @Nonnull
    private String idenfider;
    private boolean superNode;

    // -----------------------------------------------------------

    @Nullable
    private transient GridNodeMetrics metrics;
    @Nonnull
    private transient List<GridNode> replicas = new ArrayList<GridNode>(4);

    // -----------------------------------------------------------

    public GridNodeInfo() {}//for Externalizable

    public GridNodeInfo(@CheckForNull InetAddress localAddr, int port, boolean superNode) {
        this(localAddr, port, superNode, null);
    }

    public GridNodeInfo(@CheckForNull InetAddress localAddr, int port, boolean superNode, @Nullable GridNodeMetrics metrics) {
        if(localAddr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = localAddr;
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(localAddr, port);
        this.superNode = superNode;
    }

    protected GridNodeInfo(@CheckForNull GridNode copyNode, @Nullable GridNodeMetrics metrics) {
        if(copyNode == null) {
            throw new IllegalArgumentException();
        }
        this.addr = copyNode.getPhysicalAdress();
        this.port = copyNode.getPort();
        this.superNode = copyNode.isSuperNode();
        this.idenfider = copyNode.getKey();
        this.metrics = metrics;
    }

    private GridNodeInfo(@CheckForNull InetAddress addr, int port, @CheckForNull byte[] macAddr, boolean superNode) {
        if(addr == null) {
            throw new IllegalArgumentException();
        }
        if(macAddr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(macAddr, port);
        this.superNode = superNode;
    }

    public final InetAddress getPhysicalAdress() {
        return addr;
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
        return GridUtils.getNodeInfo(addr, port);
    }

    // -----------------------------------------------------------

    /**
     * 4 bytes - ip address length
     * 1 byte  - has mac address
     * 1 byte  - is super node
     * N bytes - ip addr
     * 4 bytes - port number
     * 6 bytes - mac addr (optional)
     */
    public final byte[] toBytes(boolean includeMacAddr) {
        final byte[] ip = addr.getAddress();
        byte[] ret = null;
        if(includeMacAddr) {
            final byte[] mac = NetUtils.getMacAddress(addr);
            if(mac != null && mac.length == 6) {
                byte[] id = StringUtils.getBytes(idenfider);
                ret = new byte[ip.length + 16 + id.length];
                ret[4] = (byte) 1;
                System.arraycopy(mac, 0, ret, 10 + ip.length, 6);
            }
        }
        if(ret == null) {
            ret = new byte[ip.length + 10];
            ret[4] = (byte) 0;
        }
        Primitives.putInt(ret, 0, ip.length);
        ret[5] = (byte) (superNode ? 1 : 0);
        System.arraycopy(ip, 0, ret, 6, ip.length);
        Primitives.putInt(ret, 6 + ip.length, port);
        return ret;
    }

    /**
     * 4 bytes - ip address length
     * 1 byte  - has mac address
     * 1 byte  - is super node
     * N bytes - ip addr
     * 4 bytes - port number
     * 6 bytes - mac addr (optional)
     */
    public static final GridNodeInfo fromBytes(final byte[] in) {
        final int iplen = Primitives.getInt(in, 0);
        final byte[] ip = new byte[iplen];
        boolean includeMacAddr = (in[4] == 1);
        byte[] macAddr = null;
        if(includeMacAddr) {
            macAddr = new byte[6];
            System.arraycopy(in, 10 + iplen, macAddr, 0, 6);
        }
        final boolean isSuperNode = (in[5] == 1);
        System.arraycopy(in, 6, ip, 0, iplen);
        final InetAddress addr;
        try {
            addr = InetAddress.getByAddress(ip);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        final int port = Primitives.getInt(in, 6 + iplen);
        if(macAddr == null) {
            return new GridNodeInfo(addr, port, isSuperNode);
        } else {
            return new GridNodeInfo(addr, port, macAddr, isSuperNode);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] b = IOUtils.readBytes(in);
        this.addr = InetAddress.getByAddress(b);
        this.port = in.readInt();
        this.superNode = in.readBoolean();
        this.idenfider = IOUtils.readString(in);
    }

    public static final GridNodeInfo readFrom(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        final GridNodeInfo info = new GridNodeInfo();
        info.readExternal(in);
        return info;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] b = addr.getAddress();
        IOUtils.writeBytes(b, out);
        out.writeInt(port);
        out.writeBoolean(superNode);
        IOUtils.writeString(idenfider, out);
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

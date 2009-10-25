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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import xbird.util.io.IOUtils;
import xbird.util.lang.Primitives;

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

    public GridNodeInfo() {}//for Externalizable

    public GridNodeInfo(@CheckForNull InetAddress addr, int port, boolean superNode) {
        if(addr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.port = port;
        this.idenfider = GridUtils.getNodeIdentifier(addr, port);
        this.superNode = superNode;
    }

    public GridNodeInfo(@CheckForNull GridNode copyNode) {
        if(copyNode == null) {
            throw new IllegalArgumentException();
        }
        this.addr = copyNode.getPhysicalAdress();
        this.port = copyNode.getPort();
        this.superNode = copyNode.isSuperNode();
        this.idenfider = copyNode.getKey();
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

    public GridNodeMetrics getMetrics() {
        throw new UnsupportedOperationException();
    }

    public void setMetrics(GridNodeMetrics metrics) {
        throw new UnsupportedOperationException();
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
        return idenfider;
    }

    // -----------------------------------------------------------

    public final byte[] toBytes() {
        final byte[] b = addr.getAddress();
        final byte[] ret = new byte[b.length + 9];
        Primitives.putInt(ret, 0, b.length);
        ret[4] = (byte) (superNode ? 1 : 0);
        System.arraycopy(b, 0, ret, 5, b.length);
        Primitives.putInt(ret, 5 + b.length, port);
        return ret;
    }

    public static final GridNodeInfo fromBytes(final byte[] in) {
        final int blen = Primitives.getInt(in, 0);
        final byte[] b = new byte[blen];
        boolean isSuperNode = (in[4] == 1) ? true : false;
        System.arraycopy(in, 5, b, 0, blen);
        final InetAddress addr;
        try {
            addr = InetAddress.getByAddress(b);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
        int port = Primitives.getInt(in, 5 + blen);
        return new GridNodeInfo(addr, port, isSuperNode);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] b = IOUtils.readBytes(in);
        this.addr = InetAddress.getByAddress(b);
        this.port = in.readInt();
        this.superNode = in.readBoolean();
        this.idenfider = GridUtils.getNodeIdentifier(addr, port);
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
    }

    // -----------------------------------------------------------

    public static final GridNodeInfo createInstance(final GridNode copyNode) {
        if(copyNode == null) {
            throw new IllegalArgumentException();
        }
        if(copyNode instanceof GridNodeInfo) {
            return (GridNodeInfo) copyNode;
        }
        return new GridNodeInfo(copyNode);
    }
}

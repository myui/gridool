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

import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.annotation.GridJobName;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.payload.GridNodeInfo;
import gridool.deployment.GridPerNodeClassLoader;
import gridool.deployment.PeerClassLoader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import xbird.util.annotation.AnnotationUtils;
import xbird.util.compress.LZFInputStream;
import xbird.util.compress.LZFOutputStream;
import xbird.util.datetime.StopWatch;
import xbird.util.io.FastByteArrayInputStream;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.io.FastMultiByteArrayOutputStream;
import xbird.util.io.IOUtils;
import xbird.util.lang.ClassUtils;
import xbird.util.lang.ObjectUtils;
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
public final class GridUtils {
    private static final Log LOG = LogFactory.getLog(GridUtils.class);

    private GridUtils() {}

    @Nonnull
    public static String getJobName(@Nonnull final Class<? extends GridTask> jobCls) {
        GridJobName nameAnn = AnnotationUtils.getAnnotation(jobCls, GridJobName.class);
        return nameAnn == null ? jobCls.getName() : nameAnn.value();
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static String generateJobId(@Nonnull String localNodeId, @Nonnull GridJob job) {
        final long time = System.nanoTime();
        final int hashcode = System.identityHashCode(job);
        return localNodeId + '#' + Long.toString(time) + '/' + Integer.toString(hashcode);
    }

    @Nonnull
    public static String generateTaskId(@Nonnull String jobId, @Nonnull GridTask task) {
        final long time = System.nanoTime();
        final int hashcode = System.identityHashCode(task);
        return jobId + '#' + Long.toString(time) + '/' + Integer.toString(hashcode);
    }

    public static String getNodeIdentifier(@Nonnull byte[] mac, int port) {
        return NetUtils.encodeMacAddress(mac) + ':' + port;
    }

    public static String getNodeIdentifier(@Nonnull GridNode node) {
        return getNodeIdentifier(node.getMacAdress(), node.getPort());
    }

    /**
     * @return 08-00-27-DC-4A-9E/255.255.255.255:77777 (about 39 characters)
     */
    public static String toNodeInfo(@Nonnull GridNode node) {
        byte[] macAddr = node.getMacAdress();
        String macAddrStr = NetUtils.encodeMacAddress(macAddr);
        String ipAddr = node.getPhysicalAdress().getHostAddress();
        int port = node.getPort();
        return macAddrStr + '/' + ipAddr + ':' + port;
    }

    public static GridNode fromNodeInfo(@Nonnull String nodeInfo) {
        int slashPos = nodeInfo.indexOf('/');
        if(slashPos < 1) {
            throw new IllegalArgumentException("Invalid NodeInfo format: " + nodeInfo);
        }
        String macAddrStr = nodeInfo.substring(0, slashPos);
        int colonPos = nodeInfo.indexOf(':', slashPos);
        if(colonPos < 0) {
            throw new IllegalArgumentException("Invalid NodeInfo format: " + nodeInfo);
        }
        String ipAddrStr = nodeInfo.substring(slashPos + 1, colonPos);
        String portStr = nodeInfo.substring(colonPos + 1);
        int port = Integer.parseInt(portStr);

        InetAddress ipAddr = NetUtils.getInetAddressByName(ipAddrStr);
        byte[] macAddr = NetUtils.decodeMacAddress(macAddrStr);
        return new GridNodeInfo(ipAddr, port, macAddr, false);
    }

    @Nonnull
    public static String extractJobIdFromTaskId(@Nonnull String taskId) {
        final int endIndex = taskId.lastIndexOf('#');
        if(endIndex == -1) {
            throw new IllegalArgumentException("Illegal taskId format: " + taskId);
        }
        return taskId.substring(0, endIndex);
    }

    @Nonnull
    public static InetSocketAddress getDestination(@Nonnull final GridNode node) {
        InetAddress addr = node.getPhysicalAdress();
        int port = node.getPort();
        return new InetSocketAddress(addr, port);
    }

    public static ObjectName makeMBeanName(@Nonnull final String domain, @Nonnull final String type, @Nonnull final String channelName) {
        final String mbeanName = makeMBeanNameString(domain, type, channelName);
        try {
            return new ObjectName(mbeanName);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String makeMBeanNameString(@Nonnull final String domain, @Nonnull final String type, @Nonnull final String channelName) {
        return domain + ":type=" + type + ",cluster=" + channelName;
    }

    @Nullable
    public static GridNodeInfo getNodeInfo(@Nonnull final IpAddress addr) {
        final byte[] payload = addr.getAdditionalData();
        if(payload == null) {
            return null;
        }
        return GridNodeInfo.fromBytes(payload);
    }

    @Nonnull
    public static String getGridEndpoint(@Nonnull GridNode node) {
        InetAddress addr = node.getPhysicalAdress();
        String hostname = addr.getHostName(); //NetUtils.getHostNameWithoutDomain(addr);
        return "//" + hostname + ":1099/gridool/grid-01"; // TODO non default endpoint. See gridool.server.name in xbird.properties
    }

    @Nonnull
    public static List<GridNode> selectSuperNodes(@Nonnull GridNode[] nodes) {
        final List<GridNode> list = new ArrayList<GridNode>(nodes.length / 2);
        for(GridNode n : nodes) {
            if(n.isSuperNode()) {
                list.add(n);
            }
        }
        return list;
    }

    @Nonnull
    public static byte[] generateLockKey(@Nonnull String idxName, @Nonnull byte[] key) {
        final byte[] b = StringUtils.getBytes(idxName);
        int idxNameLength = b.length;
        int keyLen = key.length;
        final byte[] ret = new byte[idxNameLength + keyLen + 2];
        System.arraycopy(b, 0, ret, 0, idxNameLength);
        ret[idxNameLength] = 0;
        ret[idxNameLength + 1] = 32;
        System.arraycopy(key, 0, ret, idxNameLength + 2, keyLen);
        return ret;
    }

    public static long getLastModified(@Nonnull Class<?> clazz) {
        final ClassLoader cl = clazz.getClassLoader();
        if(cl instanceof GridPerNodeClassLoader) {
            String clsName = clazz.getName();
            return ((GridPerNodeClassLoader) cl).getTimestamp(clsName);
        }
        return ClassUtils.getLastModified(clazz);
    }

    public static byte[] compressOutputKeys(@Nonnull final byte[][] keys) {
        final FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        final LZFOutputStream out = new LZFOutputStream(bos);
        final int size = keys.length;
        try {
            IOUtils.writeInt(size, out);
            for(int i = 0; i < size; i++) {
                byte[] k = keys[i];
                keys[i] = null;
                int klen = k.length;
                IOUtils.writeInt(klen, out);
                out.write(k, 0, klen);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bos.toByteArray_clear();
    }

    public static byte[][] uncompressOutputKeys(@Nonnull final byte[][] data) {
        final int size = data.length;
        final byte[][][] list = new byte[size][][];
        int totalKeys = 0;
        for(int i = 0; i < size; i++) {
            byte[][] k = uncompressOutputKeys(data[i]);
            data[i] = null;
            list[i] = k;
            totalKeys += k.length;
        }
        final byte[][] combined = new byte[totalKeys][];
        int destPos = 0;
        for(int i = 0; i < size; i++) {
            byte[][] k = list[i];
            list[i] = null;
            for(int j = 0; i < k.length; j++) {
                combined[destPos++] = k[j];
            }
        }
        return combined;
    }

    public static byte[][] uncompressOutputKeys(@Nonnull final byte[] data) {
        final byte[][] keys;
        try {
            FastByteArrayInputStream bis = new FastByteArrayInputStream(data);
            final LZFInputStream in = new LZFInputStream(bis);
            final int size = IOUtils.readInt(in);
            assert (size >= 0) : size;
            keys = new byte[size][];
            for(int i = 0; i < size; i++) {
                int klen = IOUtils.readInt(in);
                byte[] k = new byte[klen];
                in.read(k, 0, klen);
                keys[i] = k;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return keys;
    }

    @Nonnull
    public static ClassLoader getTaskClassLoader(@Nonnull final GridTask task, @Nonnull final ClassLoader ctxtLdr, @Nonnull final GridResourceRegistry registry) {
        final Class<? extends GridTask> taskClazz = task.getClass();
        ClassLoader cl = taskClazz.getClassLoader();
        if(cl == ctxtLdr) {
            return cl;
        }
        // Does the GridTask class exists in the parent of context ClassLoader
        final String clsName = taskClazz.getName();
        try {
            final Class<?> c = Class.forName(clsName, true, ctxtLdr);
            if(c == taskClazz) {
                return ctxtLdr;
            }
        } catch (ClassNotFoundException e) {
            ;
        }
        if(cl instanceof GridPerNodeClassLoader) {
            GridPerNodeClassLoader perNodeLdr = (GridPerNodeClassLoader) cl;
            GridNode node = perNodeLdr.getNode();
            long timestamp = perNodeLdr.getTimestamp(clsName);
            cl = new PeerClassLoader(perNodeLdr, node, timestamp, registry);
        }
        return cl;
    }

    @Nonnull
    public static byte[] getTaskKey(@Nonnull final GridTask task) {
        String key = task.getKey();
        return StringUtils.getBytes(key);
    }

    @Nonnull
    public static String generateTableName(@Nonnull final String baseName, @Nonnull final GridTask task) {
        return baseName + task.getTaskNumber();
    }

    public static byte[] toBytes(final GridCommunicationMessage msg) {
        final long startTime = System.currentTimeMillis();
        final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        try {
            IOUtils.writeInt(0, out);// allocate first 4 bytes for size
            ObjectUtils.toStreamVerbose(msg, out);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        final byte[] b = out.toByteArray();
        final int objsize = b.length - 4;
        Primitives.putInt(b, 0, objsize);

        if(LOG.isDebugEnabled()) {
            final long elapsedTime = System.currentTimeMillis() - startTime;
            LOG.debug("Elapsed time for serializing (including lazy evaluation) a GridCommunicationMessage ["
                    + msg.getMessageId()
                    + "] of "
                    + b.length
                    + " bytes: "
                    + StopWatch.elapsedTime(elapsedTime));
        }
        return b;
    }

    public static String alterFileName(String fileName, GridNode node) {
        final String addr = node.getPhysicalAdress().getHostAddress();
        final int lastIdx = fileName.length() - 1;
        final int dotpos = fileName.lastIndexOf('.');
        if(dotpos > 0 && dotpos < lastIdx) {
            final StringBuilder buf = new StringBuilder(64);
            buf.append(fileName.subSequence(0, dotpos));
            buf.append('_');
            buf.append(addr);
            buf.append(fileName.substring(dotpos));
            return buf.toString();
        } else {
            return fileName + '_' + addr;
        }
    }

    public static String generateQueryName() {
        return "q" + System.nanoTime();
    }

    public static String extractDbName(@Nonnull String dburl) {
        try {
            return dburl.substring(dburl.lastIndexOf('/') + 1);
        } catch (StringIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid DB url: " + dburl);
        }
    }

}

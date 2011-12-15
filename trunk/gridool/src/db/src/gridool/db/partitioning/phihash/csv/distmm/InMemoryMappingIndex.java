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
package gridool.db.partitioning.phihash.csv.distmm;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.db.partitioning.phihash.NodeWithPartitionNo;
import gridool.util.GridUtils;
import gridool.util.codec.VariableByteCodec;
import gridool.util.collections.IndexedSet;
import gridool.util.collections.ints.IntArrayList;
import gridool.util.collections.ints.IntHash;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.FastBufferedOutputStream;
import gridool.util.io.IOUtils;
import gridool.util.lang.ArrayUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class InMemoryMappingIndex {
    private static final Log LOG = LogFactory.getLog(InMemoryMappingIndex.class);

    private final int expectedEntries;
    private final File workdir;

    private final Map<String, Map<String, IntArrayList>> idxmap;
    private final IndexedSet<NodeWithPartitionNo> nwpset; // already resident in memory

    public InMemoryMappingIndex(int expectedEntries) {
        this.expectedEntries = expectedEntries;
        this.workdir = GridUtils.getIndexDir(false);
        this.idxmap = new HashMap<String, Map<String, IntArrayList>>(12);
        this.nwpset = new IndexedSet<NodeWithPartitionNo>(512);
    }

    public void loadIndex(String... idxNames) {
        if(idxNames.length == 0) {
            return;
        }

        for(String idxName : idxNames) {
            if(idxmap.containsKey(idxName)) {
                continue; // already loaded
            }
            File idxFile = new File(workdir, idxName);
            if(!idxFile.exists()) {
                continue;
            }
            final Map<String, IntArrayList> idx;
            try {
                idx = readFromFile(idxFile, nwpset);
            } catch (IOException e) {
                throw new IllegalStateException("failed loading index: "
                        + idxFile.getAbsolutePath(), e);
            }
            if(LOG.isInfoEnabled()) {
                LOG.info("Loaded an index from disk: " + idxFile.getAbsolutePath());
            }
            idxmap.put(idxName, idx);
        }
    }

    public void saveIndex(boolean purge, boolean override, String... idxNames) {
        final String[] names = (idxNames.length > 0) ? idxNames
                : ArrayUtils.toArray(idxmap.keySet(), String[].class);
        for(String idxName : names) {
            Map<String, IntArrayList> idx = purge ? idxmap.remove(idxName) : idxmap.get(idxName);
            if(idx != null) {
                File idxFile = new File(workdir, idxName);
                if(override || !idxFile.exists()) {
                    try {
                        writeToFile(idxFile, idx, nwpset);
                    } catch (IOException e) {
                        throw new IllegalStateException("failed to save index: "
                                + idxFile.getAbsolutePath(), e);
                    }
                    if(LOG.isInfoEnabled()) {
                        LOG.info("Saved an index to disk: " + idxFile.getAbsolutePath());
                    }
                }
            }
        }
    }

    public boolean containsIndex(final String idxName) {
        return idxmap.containsKey(idxName);
    }

    public Collection<String> getIndexNames() {
        return idxmap.keySet();
    }

    public void clearIndex(String idxName) {
        idxmap.remove(idxName);
    }

    public void clearAll() {
        idxmap.clear();
        nwpset.clear();
    }

    public synchronized Map<String, IntArrayList> getKeyMap(final String idxName) {
        Map<String, IntArrayList> keymap = idxmap.get(idxName);
        if(keymap == null) {
            keymap = new HashMap<String, IntArrayList>(expectedEntries);
            idxmap.put(idxName, keymap);
        }
        return keymap;
    }

    public synchronized void addEntry(final String key, final NodeWithPartitionNo nodeinfo, final Map<String, IntArrayList> keymap) {
        final int id = nwpset.addIndexOf(nodeinfo);
        IntArrayList list = keymap.get(key);
        if(list == null) {
            list = new IntArrayList(4);
            keymap.put(key, list);
            list.add(id);
        } else {
            if(list.indexOf(id) < 0) {
                list.add(id);
            }
        }
    }

    public synchronized void addEntry(final String idxName, final String key, final NodeWithPartitionNo nodeinfo) {
        final int id = nwpset.addIndexOf(nodeinfo);

        Map<String, IntArrayList> keymap = idxmap.get(idxName);
        if(keymap == null) {
            keymap = new HashMap<String, IntArrayList>(expectedEntries);
            idxmap.put(idxName, keymap);
            IntArrayList list = new IntArrayList(4);
            keymap.put(key, list);
            list.add(id);
        } else {
            IntArrayList list = keymap.get(key);
            if(list == null) {
                list = new IntArrayList(4);
                keymap.put(key, list);
                list.add(id);
            } else {
                if(list.indexOf(id) < 0) {
                    list.add(id);
                }
            }
        }
    }

    public synchronized void getEntries(final String idxName, final String key, final CallbackHandler handler) {
        final Map<String, IntArrayList> keymap = idxmap.get(idxName);
        if(keymap != null) {
            final IntArrayList list = keymap.get(key);
            if(list != null) {
                final int size = list.size();
                for(int i = 0; i < size; i++) {
                    int idx = list.get(i);
                    NodeWithPartitionNo nodeinfo = nwpset.get(idx);
                    handler.handle(nodeinfo);
                }
            }
        }
    }

    public interface CallbackHandler {
        void handle(NodeWithPartitionNo nodeinfo);
    }

    private static void writeToFile(final File file, final Map<String, IntArrayList> idx, final IndexedSet<NodeWithPartitionNo> nwpset)
            throws IOException {
        FileOutputStream fos = new FileOutputStream(file, false);
        FastBufferedOutputStream out = new FastBufferedOutputStream(fos, 16384);
        int atleast = nwpset.size();
        final IntHash<NodeWithPartitionNo> imap = new IntHash<NodeWithPartitionNo>(atleast);
        IOUtils.writeInt(idx.size(), out);
        for(Map.Entry<String, IntArrayList> e : idx.entrySet()) {
            String key = e.getKey();
            IOUtils.writeString(key, out);
            IntArrayList vlist = e.getValue();
            int vsize = vlist.size();
            VariableByteCodec.encodeInt(vsize, out);
            for(int i = 0; i < vsize; i++) {
                int v = vlist.fastGet(i);
                if(v >= atleast) {
                    atleast = v + 1;
                }
                VariableByteCodec.encodeInt(v, out);
                NodeWithPartitionNo pno = nwpset.get(v);
                assert (pno != null);
                imap.putIfAbsent(v, pno);
            }
        }
        IOUtils.writeInt(atleast, out);
        int imapsize = imap.size();
        IOUtils.writeInt(imapsize, out);
        for(IntHash.BucketEntry<NodeWithPartitionNo> e : imap) {
            int k = e.getKey();
            VariableByteCodec.encodeInt(k, out);
            NodeWithPartitionNo v = e.getValue();
            byte[] vb = v.getNode().toBytes();
            IOUtils.writeBytes(vb, out);
            VariableByteCodec.encodeInt(v.getPartitionNo(), out);
        }
        out.flush();
        out.close();
    }

    private static Map<String, IntArrayList> readFromFile(final File file, final IndexedSet<NodeWithPartitionNo> nwpset)
            throws IOException {
        final FileInputStream fis = new FileInputStream(file);
        final Map<String, IntArrayList> map;
        try {
            FastBufferedInputStream in = new FastBufferedInputStream(fis, 16384);
            final int size = IOUtils.readInt(in);
            map = new HashMap<String, IntArrayList>(size);
            for(int i = 0; i < size; i++) {
                String key = IOUtils.readString(in);
                final int vsize = VariableByteCodec.decodeInt(in);
                final int[] varray = new int[vsize];
                for(int j = 0; j < vsize; j++) {
                    varray[j] = VariableByteCodec.decodeInt(in);
                }
                IntArrayList value = new IntArrayList(varray);
                map.put(key, value);
            }
            int atLeast = IOUtils.readInt(in);
            assert (atLeast >= 0) : atLeast;
            nwpset.ensureSize(atLeast);
            final int imapSize = IOUtils.readInt(in);
            for(int j = 0; j < imapSize; j++) {
                int k = VariableByteCodec.decodeInt(in);
                byte[] vb = IOUtils.readBytes(in);
                GridNode node = GridNodeInfo.fromBytes(vb);
                int partitionNo = VariableByteCodec.decodeInt(in);
                NodeWithPartitionNo v = new NodeWithPartitionNo(node, partitionNo);
                nwpset.set(k, v);
            }
            in.close();
        } finally {
            fis.close();
        }
        return map;
    }

}

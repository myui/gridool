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
package gridool.db.partitioning.csv.distmm;

import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.Settings;
import gridool.cache.GridCacheManager;
import gridool.db.partitioning.NodeWithPartitionNo;
import gridool.util.GridUtils;
import gridool.util.codec.VariableByteCodec;
import gridool.util.collections.LRUMap;
import gridool.util.collections.ints.IntArrayList;
import gridool.util.compress.ContinousInflaterInputStream;
import gridool.util.compress.DeflaterOutputStream;
import gridool.util.hashes.FNVHash;
import gridool.util.hashes.HashUtils;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.FastBufferedOutputStream;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.io.IOUtils;
import gridool.util.primitive.Primitives;
import gridool.util.string.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class InMemoryIndexHelper {
    private static final Log LOG = LogFactory.getLog(InMemoryIndexHelper.class);

    private static final int FKCHUNK_IO_BUFSIZE;
    private static final int COMPRESSOR_BUFSIZE;
    private static final int CACHED_BUCKETS;
    static {
        FKCHUNK_IO_BUFSIZE = Primitives.parseInt(Settings.get("gridool.db.partitioning.fkchunk.io.bufsize"), 8 * 1024);
        COMPRESSOR_BUFSIZE = Primitives.parseInt(Settings.get("gridool.db.partitioning.fkchunk.compressor.bufsize"), 16 * 1024);
        CACHED_BUCKETS = Primitives.parseInt(Settings.get("gridool.db.partitioning.mmindex.cached_hashbuckets"), 3);
    }

    private InMemoryIndexHelper() {}

    public static void writeToFile(final String distkey, final NodeWithPartitionNo nodeInfo, final String fkIdxName, final Map<String, OutputStream> outputMap, final int bucketShift)
            throws IOException {
        final byte[] k = StringUtils.getBytes(distkey);
        final byte[] v = nodeInfo.serialize();
        int hashcode = FNVHash.hash32(k);
        final int bucket = HashUtils.positiveXorFolding(hashcode, bucketShift);

        final OutputStream out;
        synchronized(outputMap) {
            out = prepareFkOutputStream(fkIdxName, bucket, outputMap);
        }

        if(out != null) {
            synchronized(out) {
                VariableByteCodec.encodeInt(k.length, out);
                VariableByteCodec.encodeInt(v.length, out);
                out.write(k);
                out.write(v);
            }
        }
    }

    public static void writeToStream(final byte[] k, final NodeWithPartitionNo nodeInfo, final String fkIdxName, final FastByteArrayOutputStream out, final int bucketShift)
            throws IOException {
        IOUtils.writeString(fkIdxName, out);
        int hashcode = FNVHash.hash32(k);
        int bucket = HashUtils.positiveXorFolding(hashcode, bucketShift);
        IOUtils.writeInt(bucket, out);

        byte[] v = nodeInfo.serialize();
        int klen = k.length;
        int vlen = v.length;
        int recordLength = VariableByteCodec.requiredBytes(klen)
                + VariableByteCodec.requiredBytes(vlen) + klen + vlen;
        IOUtils.writeInt(recordLength, out);

        VariableByteCodec.encodeInt(klen, out);
        VariableByteCodec.encodeInt(vlen, out);
        out.write(k);
        out.write(v);
    }

    /**
     * Synchronization is required.
     */
    public static void writeToFile(final InputStream in) throws IOException {
        final byte[] recordBuf = new byte[2048]; // big buffer enough for a record
        final Map<String, OutputStream> outputMap = new HashMap<String, OutputStream>(12);
        while(in.available() > 0) {
            String fkIdxName = IOUtils.readString(in);
            int bucket = IOUtils.readInt(in);
            int recordlen = IOUtils.readInt(in);
            IOUtils.readFully(in, recordBuf, 0, recordlen);

            OutputStream out = prepareFkOutputStream(fkIdxName, bucket, outputMap);
            out.write(recordBuf, 0, recordlen);
        }
        for(OutputStream out : outputMap.values()) {
            out.flush();
            out.close();
        }
    }

    public static void loadIndex(final InMemoryMappingIndex index, final String[] fkIdxNames, final int bucket, final boolean loadAll)
            throws GridException {
        for(final String fkIdxName : fkIdxNames) {
            if(!loadAll) {
                if(index.containsIndex(fkIdxName)) {
                    continue;
                }
            }
            final Map<String, IntArrayList> keymap = index.getKeyMap(fkIdxName);
            File file = getFkIndexFile(fkIdxName, bucket);
            if(!file.exists()) {
                LOG.warn("Loading index failed because a chunk FK index file does not exist: "
                        + file.getAbsolutePath());
                continue;
            }
            final FileInputStream fis;
            try {
                fis = new FileInputStream(file);
            } catch (FileNotFoundException fne) {
                throw new GridException("FK chunk index file not found: " + file.getAbsolutePath(), fne);
            }
            InflaterInputStream zin = new ContinousInflaterInputStream(fis, new Inflater(false), COMPRESSOR_BUFSIZE);
            final FastBufferedInputStream in = new FastBufferedInputStream(zin, FKCHUNK_IO_BUFSIZE);
            try {
                int b;
                while((b = in.read()) != -1) {
                    int distkeylen = VariableByteCodec.decodeInt(in, b);
                    int valuelen = VariableByteCodec.decodeInt(in);
                    byte[] k = new byte[distkeylen];
                    in.read(k, 0, distkeylen);
                    byte[] v = new byte[valuelen];
                    in.read(v, 0, valuelen);
                    String distkey = StringUtils.toString(k, 0, distkeylen);
                    NodeWithPartitionNo nodeinfo = NodeWithPartitionNo.deserialize(v);
                    index.addEntry(distkey, nodeinfo, keymap);
                }
            } catch (IOException ioe) {
                String errmsg = "Failed to load a chunk FK index: " + file.getAbsolutePath();
                LOG.error(errmsg, ioe);
                throw new GridException(errmsg, ioe);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
    }

    private static OutputStream prepareFkOutputStream(final String fkIdxName, final int bucket, final Map<String, OutputStream> outputMap)
            throws IOException {
        String fname = fkIdxName + bucket + ".fk.gz";
        OutputStream out = outputMap.get(fname);
        if(out == null) {
            File file = getFkIndexFile(fname);
            FileOutputStream fos = new FileOutputStream(file, true);
            DeflaterOutputStream zos = new DeflaterOutputStream(fos, new Deflater(Deflater.DEFAULT_COMPRESSION, false), COMPRESSOR_BUFSIZE);
            out = new FastBufferedOutputStream(zos, FKCHUNK_IO_BUFSIZE);
            outputMap.put(fname, out);
        }
        return out;
    }

    private static File getFkIndexFile(final String fname) {
        File dir = GridUtils.getWorkDir(false);
        File file = new File(dir, fname);
        return file;
    }

    private static File getFkIndexFile(final String idxName, final int bucket) {
        String fname = idxName + bucket + ".fk.gz";
        File dir = GridUtils.getWorkDir(false);
        File file = new File(dir, fname);
        return file;
    }

    /**
     * loading indices for the buckets.
     */
    public static InMemoryMappingIndex loadIndex(final int bucket, final String[] parentTableFkIndexNames, final GridResourceRegistry registry)
            throws GridException {
        GridCacheManager localCache = registry.getLocalCache();
        LRUMap<String, InMemoryMappingIndex> idxCache = localCache.buildCache(InMemoryMappingIndex.class.getSimpleName(), CACHED_BUCKETS);

        String idxName = "InMemoryMappingIndex#" + bucket;
        InMemoryMappingIndex idx = idxCache.get(idxName);
        if(idx == null) {
            idx = new InMemoryMappingIndex(10000);
            loadIndex(idx, parentTableFkIndexNames, bucket, true);
            idxCache.put(idxName, idx);
        } else {
            loadIndex(idx, parentTableFkIndexNames, bucket, false);
        }
        return idx;
    }

}

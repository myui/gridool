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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.catalog.PartitionKey;
import gridool.db.helpers.ConstraintKey;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.FileAppendTask;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import xbird.util.collections.FixedArrayList;
import xbird.util.csv.CsvUtils;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.io.IOUtils;
import xbird.util.primitive.MutableInt;
import xbird.util.string.StringUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CsvHashPartitioningJob extends
        GridJobBase<CsvHashPartitioningJob.JobConf, Map<GridNode, MutableInt>> {
    private static final long serialVersionUID = 149683992715077498L;

    private transient Map<GridNode, MutableInt> assignedRecMap;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public CsvHashPartitioningJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final CsvHashPartitioningJob.JobConf ops)
            throws GridException {
        assert (registry != null);
        final String[] lines = ops.getLines();
        final String csvFileName = ops.getFileName();
        final boolean append = !ops.isFirst();
        final DBPartitioningJobConf jobConf = ops.getJobConf();

        final PartitionKey primaryPartitionKey;
        final List<PartitionKey> foreignPartitionKeys;
        final int pkeyPartitionNo;
        final int[] pkeyIndicies;
        {
            DistributionCatalog catalog = registry.getDistributionCatalog();
            String baseTableName = jobConf.getBaseTableName();
            String actualTableName = jobConf.getTableName();
            Pair<PartitionKey, List<PartitionKey>> partitioningKeys = catalog.bindPartitioningKeyPositions(baseTableName, actualTableName);
            primaryPartitionKey = partitioningKeys.getFirst();
            foreignPartitionKeys = partitioningKeys.getSecond();            
            pkeyPartitionNo = primaryPartitionKey.getPartitionNo();
            PrimaryKey primaryKey = primaryPartitionKey.getKey();
            pkeyIndicies = primaryKey.getColumnPositions(true);
        }

        final boolean insertHiddenField = jobConf.insertHiddenField();
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        final String[] fields = new String[getMaxColumnCount(primaryPartitionKey, foreignPartitionKeys)];
        assert (fields.length > 0);
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);

        final Charset charset = Charset.forName("UTF-8");
        final StringBuilder strBuf = new StringBuilder(64);
        final int totalRecords = lines.length;
        final int numNodes = router.getGridSize();
        final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap = new IdentityHashMap<GridNode, Pair<MutableInt, FastByteArrayOutputStream>>(numNodes);
        final Map<GridNode, MutableInt> mappedNodes = new IdentityHashMap<GridNode, MutableInt>(numNodes);
        for(int i = 0; i < totalRecords; i++) {
            int bitShift = 0;
            String line = lines[i];
            lines[i] = null;
            final byte[] lineBytes = line.getBytes(charset);
            {//primary mapping
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                String pkeysField = combineFields(fields, strBuf);
                decideRecordMapping(router, mappedNodes, pkeysField, pkeyPartitionNo);
                bitShift++;
            }
            if(fkeyIndicies != null) {
                CsvUtils.retrieveFields(line, fkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                decideRecordMapping(router, mappedNodes, bitShift, fields);
            }

            if(mappedNodes.isEmpty()) {
                throw new IllegalStateException("Could not map records because there is neither PK nor FK in table '"
                        + jobConf.getTableName() + '\'');
            }
            mapRecord(lineBytes, totalRecords, numNodes, nodeAssignMap, mappedNodes, insertHiddenField, filedSeparator);
            mappedNodes.clear();
        }

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        final Map<GridNode, MutableInt> assignedRecMap = new IdentityHashMap<GridNode, MutableInt>(numNodes);
        for(final Map.Entry<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> e : nodeAssignMap.entrySet()) {
            GridNode node = e.getKey();
            Pair<MutableInt, FastByteArrayOutputStream> pair = e.getValue();
            MutableInt numRecords = pair.first;
            assignedRecMap.put(node, numRecords);
            FastByteArrayOutputStream rows = pair.second;
            byte[] b = rows.toByteArray();
            pair.clear();
            GridTask task = new FileAppendTask(this, csvFileName, b, append, true);
            map.put(task, node);
        }

        for(final GridNode node : router.getAllNodes()) {
            if(!assignedRecMap.containsKey(node)) {
                assignedRecMap.put(node, new MutableInt(0));
            }
        }
        this.assignedRecMap = assignedRecMap;
        return map;
    }

    private static void decideRecordMapping(final GridTaskRouter router, final Map<GridNode, MutableInt> mappedNodes, final String field, final int partitionNo)
            throws GridException {
        final byte[] k = StringUtils.getBytes(field);
        final GridNode node = router.selectNode(k);
        if(node == null) {
            throw new GridException("Could not find any node in cluster.");
        }
        MutableInt hiddenMapping = mappedNodes.get(node);
        if(hiddenMapping == null) {
            mappedNodes.put(node, new MutableInt(partitionNo));
        } else {
            int newValue = hiddenMapping.intValue() | partitionNo;
            hiddenMapping.setValue(newValue);
        }
    }

    private static void mapRecord(final byte[] line, final int totalRecords, final int numNodes, final Map<GridNode, Pair<MutableInt, FastByteArrayOutputStream>> nodeAssignMap, final Map<GridNode, MutableInt> mappedNodes, final boolean insertHiddenField, final char filedSeparator) {
        final int lineSize = line.length;
        for(Map.Entry<GridNode, MutableInt> e : mappedNodes.entrySet()) {
            final GridNode node = e.getKey();
            final int hiddenValue = e.getValue().intValue();

            final FastByteArrayOutputStream rowsBuf;
            final Pair<MutableInt, FastByteArrayOutputStream> pair = nodeAssignMap.get(node);
            if(pair == null) {
                int expected = (int) ((lineSize * (totalRecords / numNodes)) * 1.3f);
                rowsBuf = new FastByteArrayOutputStream(Math.max(expected, 32786));
                Pair<MutableInt, FastByteArrayOutputStream> newPair = new Pair<MutableInt, FastByteArrayOutputStream>(new MutableInt(1), rowsBuf);
                nodeAssignMap.put(node, newPair);
            } else {
                rowsBuf = pair.second;
                MutableInt cnt = pair.first;
                cnt.increment();
            }
            rowsBuf.write(line, 0, lineSize);
            if(insertHiddenField) {
                // FIXME workaround for MonetDB 
                final String str = Integer.toString(hiddenValue);
                final int strlen = str.length();
                for(int i = 0; i < strlen; i++) {
                    char c = str.charAt(i);
                    rowsBuf.write(c);
                }
                rowsBuf.write(filedSeparator);
            }
            rowsBuf.write('\n'); // TODO FIXME support other record separator 
        }
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Map<GridNode, MutableInt> reduce() throws GridException {
        return assignedRecMap;
    }

    static final class JobConf implements Externalizable {

        private String[] lines;
        private String fileName;
        private boolean isFirst;
        private DBPartitioningJobConf jobConf;

        public JobConf() {//for Externalizable
            super();
        }

        public JobConf(@Nonnull String[] lines, @Nonnull String fileName, boolean isFirst, @Nonnull DBPartitioningJobConf jobConf) {
            this.lines = lines;
            this.fileName = fileName;
            this.isFirst = isFirst;
            this.jobConf = jobConf;
        }

        String[] getLines() {
            return lines;
        }

        String getFileName() {
            return fileName;
        }

        boolean isFirst() {
            return isFirst;
        }

        DBPartitioningJobConf getJobConf() {
            return jobConf;
        }

        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            final int num = in.readInt();
            final String[] ary = new String[num];
            for(int i = 0; i < num; i++) {
                ary[i] = IOUtils.readString(in);
            }
            this.lines = ary;
            this.fileName = IOUtils.readString(in);
            this.isFirst = in.readBoolean();
            this.jobConf = (DBPartitioningJobConf) in.readObject();
        }

        public void writeExternal(final ObjectOutput out) throws IOException {
            final String[] ary = lines;
            final int num = ary.length;
            out.writeInt(num);
            for(int i = 0; i < num; i++) {
                IOUtils.writeString(ary[i], out);
            }
            IOUtils.writeString(fileName, out);
            out.writeBoolean(isFirst);
            out.writeObject(jobConf);
        }
    }

    private static int getMaxColumnCount(@Nonnull final PartitionKey primaryPartitionKey, @Nonnull final List<PartitionKey> foreignPartitionKeys) {
        int max = primaryPartitionKey.getKey().getColumnNames().size();
        for(final PartitionKey key : foreignPartitionKeys) {
            int size = key.getKey().getColumnNames().size();
            max = Math.max(size, max);
        }
        return max;
    }

    private static String combineFields(@Nonnull final String[] fields, @Nonnull final StringBuilder buf) {
        final int numFields = fields.length;
        if(numFields == 0) {
            throw new IllegalArgumentException();
        }
        if(numFields == 1) {
            return fields[0];
        }
        StringUtils.clear(buf);
        buf.append(fields[0]);
        for(int i = 1; i < numFields; i++) {
            buf.append('|');
            buf.append(fields[i]);
        }
        return buf.toString();
    }
}

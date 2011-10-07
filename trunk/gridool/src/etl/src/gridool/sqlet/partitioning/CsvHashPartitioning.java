/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.sqlet.partitioning;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.routing.GridRouter;
import gridool.sqlet.partitioning.PartitioningConf.Partition;
import gridool.util.collections.FixedArrayList;
import gridool.util.csv.CsvReader;
import gridool.util.csv.CsvUtils;
import gridool.util.csv.SimpleCsvReader;
import gridool.util.io.FastBufferedInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
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
public final class CsvHashPartitioning extends
        GridJobBase<CsvHashPartitioning.CsvHashPartitioningConf, PartitioningResult> {
    private static final long serialVersionUID = -7966712759745436372L;
    private static final Log LOG = LogFactory.getLog(CsvHashPartitioning.class);

    @Override
    public Map<GridTask, GridNode> map(GridRouter router, CsvHashPartitioningConf conf)
            throws GridException {
        List<Partition> partitions = conf.getPartitions();
        if(partitions.isEmpty()) {
            throw new GridException("No partition is defined");
        }

        final char filedSeparator = conf.filedSeparator;
        final char quoteChar = conf.quoteChar;
        final int[] partitioningColumns = conf.partitioningColumns;
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(partitioningColumns.length);

        final CsvReader reader = getCsvReader(conf.csvFile, filedSeparator, quoteChar);
        try {
            String line;
            while((line = reader.getNextLine()) != null) {
                CsvUtils.retrieveFields(line, partitioningColumns, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                
            }
        } catch (IOException e) {
        }

        return null;
    }

    @Override
    public PartitioningResult reduce() throws GridException {
        return null;
    }

    private static final CsvReader getCsvReader(final String csvPath, final char filedSeparator, final char quoteChar)
            throws GridException {
        final Reader reader;
        try {
            FileInputStream fis = new FileInputStream(csvPath);
            FastBufferedInputStream bis = new FastBufferedInputStream(fis, 16384);
            reader = new InputStreamReader(bis, "UTF-8");
        } catch (FileNotFoundException fne) {
            LOG.error(fne);
            throw new GridException("CSV file not found: " + csvPath, fne);
        } catch (UnsupportedEncodingException uee) {
            LOG.error(uee);
            throw new IllegalStateException(uee); // should never happens
        }
        PushbackReader pushback = new PushbackReader(reader);
        return new SimpleCsvReader(pushback, filedSeparator, quoteChar);
    }

    public static final class CsvHashPartitioningConf extends PartitioningConf {
        private static final long serialVersionUID = -6527968827015782880L;

        final String csvFile;
        final int[] partitioningColumns;

        char filedSeparator = '\t';
        char quoteChar = '\"';

        public CsvHashPartitioningConf(String csvFile, int[] partitioningColumns) {
            super();
            this.csvFile = csvFile;
            this.partitioningColumns = partitioningColumns;
        }

        public void setFiledSeparator(char filedSeparator) {
            this.filedSeparator = filedSeparator;
        }

        public void setQuoteChar(char quoteChar) {
            this.quoteChar = quoteChar;
        }

    }

}

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
package gridool.lib.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.construct.GridTaskAdapter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import xbird.util.csv.CsvReader;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class CsvHashPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4477383489963213348L;

    protected final BulkloadJobConf jobConf;

    @SuppressWarnings("unchecked")
    public CsvHashPartitioningTask(GridJob job, BulkloadJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
    }

    public Serializable execute() throws GridException {

        CsvReader reader = getCsvReader(jobConf);
        String[] nextLine;
        while((nextLine = reader.readNext()) != null) {
            
        }

        return null;
    }

    private static final CsvReader getCsvReader(final BulkloadJobConf jobConf) throws GridException {
        final String csvPath = jobConf.getCsvFilePath();
        final Reader reader;
        try {
            reader = new InputStreamReader(new FileInputStream(csvPath), "UTF-8");
        } catch (FileNotFoundException fne) {
            throw new GridException("CSV file not found: " + csvPath, fne);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalStateException(uee);   // should never happens
        }
        return new CsvReader(reader, jobConf.getFieldSeparator(), jobConf.getStringQuote(), jobConf.getEscapeCharacter());
    }

}

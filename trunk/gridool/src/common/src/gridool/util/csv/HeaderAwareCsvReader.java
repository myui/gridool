/*
 * @(#)$Id$
 *
 * Copyright 2010-2011 Makoto YUI
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
package gridool.util.csv;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Makoto YUI
 */
public final class HeaderAwareCsvReader extends SimpleCsvReader {

    private Map<String, Integer> header = null;

    private String[] line = null;
    private boolean reachedEnd = false;

    public HeaderAwareCsvReader(Reader reader, char fieldSeparator, char quoteChar) {
        super(reader, fieldSeparator, quoteChar);
    }

    public HeaderAwareCsvReader(PushbackReader reader, char fieldSeparator, char quoteChar) {
        super(reader, fieldSeparator, quoteChar);
    }

    /**
     * Parse header must be called before {@link #next()}.
     */
    public Map<String, Integer> parseHeader() throws IOException {
        if(header == null) {
            if(next()) {
                header = new HashMap<String, Integer>(line.length);
                for(int i = 0; i < line.length; i++) {
                    header.put(line[i].toUpperCase(), i);
                }
                return header;
            }
        }
        return null;
    }

    public boolean next() {
        if(reachedEnd) {
            return false;
        }
        final String[] nextLine;
        try {
            nextLine = readNext();
        } catch (IOException e) {
            this.reachedEnd = true;
            return false;
        }
        if(nextLine == null) {
            this.reachedEnd = true;
            return false;
        } else {
            this.line = nextLine;
            return true;
        }
    }

    public String[] getLine() {
        return line;
    }

    public String get(int i) {
        if(line == null) {
            throw new IllegalStateException("Current line is not set");
        }
        if(i >= line.length) {
            return null; // For the case where last column is null
        }
        return line[i];
    }

    public String get(String colname) {
        if(header == null) {
            throw new IllegalStateException("CSV header is not parsed");
        }
        Integer col = header.get(colname);
        if(col == null) {
            throw new IllegalArgumentException("Column not found: " + colname);
        }
        int i = col.intValue();
        return get(i);
    }

}

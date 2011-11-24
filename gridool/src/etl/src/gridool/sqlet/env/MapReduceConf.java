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
package gridool.sqlet.env;

import gridool.GridNode;
import gridool.sqlet.SqletException;
import gridool.sqlet.SqletException.ErrorType;
import gridool.util.GridUtils;
import gridool.util.csv.HeaderAwareCsvReader;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.IOUtils;
import gridool.util.lang.Preconditions;
import gridool.util.primitive.Primitives;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Makoto YUI
 */
public class MapReduceConf implements Serializable {
    private static final long serialVersionUID = -951607371007537258L;

    private final List<Reducer> reducers;
    private final Comparator<Reducer> comparator;

    public MapReduceConf() {
        this.reducers = new ArrayList<MapReduceConf.Reducer>();
        this.comparator = new DefaultComparator();
    }

    public List<Reducer> getReducers(boolean sortByPriority) {
        if(sortByPriority) {
            return getReducers(comparator);
        } else {
            return reducers;
        }
    }

    public List<Reducer> getReducers(Comparator<Reducer> comparator) {
        Collections.sort(reducers, comparator);
        return reducers;
    }

    public void loadReducers(String uri) throws SqletException {
        if(uri.endsWith(".csv")) {
            final InputStream is;
            try {
                is = IOUtils.openStream(uri);
            } catch (IOException e) {
                throw new SqletException(ErrorType.configFailed, "Illegal URI format: " + uri, e);
            }
            InputStreamReader reader = new InputStreamReader(new FastBufferedInputStream(is));
            HeaderAwareCsvReader csvReader = new HeaderAwareCsvReader(reader, ',', '"');

            final Map<String, Integer> headerMap;
            try {
                headerMap = csvReader.parseHeader();
            } catch (IOException e) {
                throw new SqletException(ErrorType.configFailed, "failed to parse a header: " + uri, e);
            }

            final int[] fieldIndexes = toFieldIndexes(headerMap);
            while(csvReader.next()) {
                String host = csvReader.get(fieldIndexes[0]);
                String portStr = csvReader.get(fieldIndexes[1]);
                String dbUrl = csvReader.get(fieldIndexes[2]);
                String shuffleDataSink = csvReader.get(fieldIndexes[3]);
                String priorityStr = csvReader.get(fieldIndexes[4]);

                Preconditions.checkNotNull(host, portStr);

                int port = Integer.parseInt(portStr);
                GridNode hostNode = GridUtils.getNode(host, port);
                int priority = Primitives.parseInt(priorityStr, -1);
                Reducer r = new Reducer(hostNode, dbUrl, shuffleDataSink);
                r.setPriority(priority);
                reducers.add(r);
            }
        } else {
            throw new IllegalArgumentException("Unsupported URI: " + uri);
        }
    }

    private static int[] toFieldIndexes(@Nullable Map<String, Integer> map) {
        if(map == null) {
            return new int[] { 0, 1, 2, 3, 4 };
        } else {
            Integer c0 = map.get("HOST");
            Integer c1 = map.get("PORT");
            Integer c2 = map.get("DBURL");
            Integer c3 = map.get("SHUFFLEDATASINK");
            Integer c4 = map.get("PRIORITY");

            Preconditions.checkNotNull(c0, c1, c2, c3, c4);

            final int[] indexes = new int[5];
            indexes[0] = c0.intValue();
            indexes[1] = c1.intValue();
            indexes[2] = c2.intValue();
            indexes[3] = c3.intValue();
            indexes[4] = c4.intValue();
            return indexes;
        }
    }

    public static final class Reducer implements Serializable {
        private static final long serialVersionUID = 7657523061627358443L;

        @Nonnull
        final GridNode host;
        @Nullable
        final String dbUrl;
        @Nullable
        final String shuffleDataSink;

        @Nonnegative
        int priority = Integer.MAX_VALUE; // smaller is bigger (always priority > 0)

        public Reducer(@Nonnull GridNode host, @Nullable String dbUrl, @Nullable String shuffleDataSink) {
            this.host = host;
            this.dbUrl = dbUrl;
            this.shuffleDataSink = shuffleDataSink;
        }

        public void setPriority(@Nonnegative int priority) {
            if(priority <= 0) {
                this.priority = Integer.MAX_VALUE;
            } else {
                this.priority = priority;
            }
        }

        @Override
        public String toString() {
            return "Reducer [host=" + host + ", dbUrl=" + dbUrl + ", shuffleDataSink="
                    + shuffleDataSink + ", priority=" + priority + "]";
        }

    }

    public static final class DefaultComparator implements Comparator<Reducer> {

        public DefaultComparator() {}

        @Override
        public int compare(final Reducer lhs, final Reducer rhs) {
            int thisVal = lhs.priority;
            int anotherVal = rhs.priority;
            return (thisVal == anotherVal ? 0 : (thisVal < anotherVal ? -1 : 1));
        }

    }

}

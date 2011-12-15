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
package gridool.sqlet.catalog;

import gridool.GridNode;
import gridool.sqlet.SqletException;
import gridool.sqlet.SqletException.SqletErrorType;
import gridool.util.GridUtils;
import gridool.util.csv.HeaderAwareCsvReader;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.lang.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;

/**
 * @author Makoto YUI
 */
public class MapReduceConf implements Serializable {
    private static final long serialVersionUID = -951607371007537258L;

    private final List<Reducer> reducers;

    public MapReduceConf() {
        this.reducers = new ArrayList<MapReduceConf.Reducer>();
    }

    public List<Reducer> getReducers() {
        return reducers;
    }

    public List<Reducer> getReducers(Comparator<Reducer> comparator) {
        Collections.sort(reducers, comparator);
        return reducers;
    }

    public void loadReducers(String uri) throws SqletException {
        if(uri.endsWith(".csv") || uri.endsWith(".CSV")) {
            final InputStream is;
            try {
                FileSystemManager fsManager = VFS.getManager();
                FileObject fileObj = fsManager.resolveFile(uri);
                FileContent fileContent = fileObj.getContent();
                is = fileContent.getInputStream();
            } catch (FileSystemException e) {
                throw new SqletException(SqletErrorType.configFailed, "failed to load a file: "
                        + uri, e);
            }
            InputStreamReader reader = new InputStreamReader(new FastBufferedInputStream(is));
            HeaderAwareCsvReader csvReader = new HeaderAwareCsvReader(reader, ',', '"');

            final Map<String, Integer> headerMap;
            try {
                headerMap = csvReader.parseHeader();
            } catch (IOException e) {
                throw new SqletException(SqletErrorType.configFailed, "failed to parse a header: "
                        + uri, e);
            }

            final int[] fieldIndexes = toFieldIndexes(headerMap);
            while(csvReader.next()) {
                String id = csvReader.get(fieldIndexes[0]);
                String nodeStr = csvReader.get(fieldIndexes[1]);
                String dbUrl = csvReader.get(fieldIndexes[2]);
                String shuffleDataSink = csvReader.get(fieldIndexes[3]);

                Preconditions.checkNotNull(id, nodeStr);

                GridNode hostNode = GridUtils.getNode(nodeStr);
                Reducer r = new Reducer(id, hostNode, dbUrl, shuffleDataSink);
                reducers.add(r);
            }
        } else {
            throw new IllegalArgumentException("Unsupported URI: " + uri);
        }
    }

    private static int[] toFieldIndexes(@Nullable Map<String, Integer> map) {
        if(map == null) {
            return new int[] { 0, 1, 2, 3, };
        } else {
            Integer c0 = map.get("ID");
            Integer c1 = map.get("NODE");
            Integer c2 = map.get("DBURL");
            Integer c3 = map.get("SHUFFLEDATASINK");

            Preconditions.checkNotNull(c0, c1, c2, c3);

            final int[] indexes = new int[4];
            indexes[0] = c0.intValue();
            indexes[1] = c1.intValue();
            indexes[2] = c2.intValue();
            indexes[3] = c3.intValue();
            return indexes;
        }
    }

    public static final class Reducer implements Serializable {
        private static final long serialVersionUID = 7657523061627358443L;

        @Nonnull
        final String id;
        @Nonnull
        final GridNode host;
        @Nullable
        final String dbUrl;
        @Nullable
        final String shuffleDataSink;

        public Reducer(@Nonnull String id, @Nonnull GridNode host, @Nullable String dbUrl, @Nullable String shuffleDataSink) {
            this.id = id;
            this.host = host;
            this.dbUrl = dbUrl;
            this.shuffleDataSink = shuffleDataSink;
        }

        @Override
        public String toString() {
            return "Reducer [id=" + id + ", host=" + host + ", dbUrl=" + dbUrl
                    + ", shuffleDataSink=" + shuffleDataSink + "]";
        }

    }

    @Override
    public String toString() {
        return reducers.toString();
    }

}

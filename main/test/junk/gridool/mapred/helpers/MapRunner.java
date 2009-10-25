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
package gridool.mapred.helpers;

import java.io.IOException;

import javax.annotation.Nonnull;

import gridool.mapred.MapReduceException;
import gridool.mapred.Mapper;
import gridool.mapred.OutputCollector;
import gridool.mapred.io.RecordReader;
import gridool.util.WritableComparable;
import xbird.util.io.Writable;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class MapRunner<K1 extends WritableComparable, V1 extends Writable, K2 extends WritableComparable, V2 extends Writable>
        implements MapRunnable<K1, V1, K2, V2> {

    private final Mapper<K1, V1, K2, V2> mapper;

    public MapRunner(@Nonnull Mapper<K1, V1, K2, V2> mapper) {
        if(mapper == null) {
            throw new IllegalArgumentException();
        }
        this.mapper = mapper;
    }

    public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output)
            throws MapReduceException {
        final Pair<K1, V1> entry = new Pair<K1, V1>();
        try {
            while(input.next(entry)) {
                K1 key = entry.getFirst();
                V1 value = entry.getSecond();
                mapper.map(key, value, output);
            }
        } finally {
            try {
                mapper.close();
            } catch (IOException e) {
                throw new MapReduceException("Failed to shutdown a mapper instance: " + mapper, e);
            }
        }
    }
}

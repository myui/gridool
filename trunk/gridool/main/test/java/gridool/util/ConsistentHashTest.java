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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import xbird.util.lang.HashAlgorithm;
import xbird.util.math.MathUtils;
import xbird.util.primitives.MutableInt;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class ConsistentHashTest {
    private static final boolean prettyPrint = false;

    @Test
    public void testAdd() {
        int objects = 25923;//30000;//10000;
        int numNodes = 64;//10;
        int[] numReplicas = new int[] { 1, 2, 4, 8, 16, 32, 64, 128, 256, 512 };
        HashAlgorithm[] algos = new HashAlgorithm[] { HashAlgorithm.CRC32_HASH,
                HashAlgorithm.FNV1_32_HASH, HashAlgorithm.FNV1_64_HASH,
                HashAlgorithm.FNV1A_32_HASH, HashAlgorithm.FNV1A_64_HASH, HashAlgorithm.SHA1_HASH,
                HashAlgorithm.MD5_HASH, HashAlgorithm.NATIVE_HASH };
        for(HashAlgorithm algo : algos) {
            System.err.println("[HashAlgorithm: " + algo.name() + "]");
            for(int replica : numReplicas) {
                doTest(objects, numNodes, replica, algo);
            }
            System.out.flush();
            System.err.flush();
        }

    }

    private void doTest(int objects, int nodes, int replica, HashAlgorithm algo) {
        final CHash chash = new CHash(nodes, replica, algo);

        final Random rand = new Random(344455097234969520L);
        for(int i = 0; i < objects; i++) {
            Double d = rand.nextGaussian();
            chash.add(d.toString());
        }

        int[] counts = chash.getCounts();
        float mean = objects / nodes;
        float sd = MathUtils.stddev(counts);
        float percent = (sd / mean) * 100f;
        if(prettyPrint) {
            System.out.println("replica: " + replica + ", stddev: " + sd + " ( " + percent + "% )");
        } else {
            System.out.println(percent);
        }
    }

    private static class CHash {

        final HashFunction hashfunc;

        final List<MutableInt> counters;
        final TreeMap<Long, MutableInt> circle = new TreeMap<Long, MutableInt>();

        final int replicas;

        CHash(int nodes, int replicas, HashAlgorithm algo) {
            this.hashfunc = new DefaultHashFunction(algo);
            this.replicas = replicas;
            this.counters = new ArrayList<MutableInt>(nodes);
            prepareNodes(nodes);
        }

        void prepareNodes(int nodes) {
            final Random rand = new Random(4343898346279952L);
            for(int i = 0; i < nodes; i++) {
                Double d = rand.nextGaussian();
                String k = d.toString();

                final MutableInt counter = new MutableInt(0);
                counters.add(counter);

                Long h = hashfunc.hash(k);
                circle.put(h, counter);
                for(int j = 1; j <= replicas; j++) {
                    h = hashfunc.hash(k + j);
                    circle.put(h, counter);
                }
            }
        }

        void add(String key) {
            MutableInt counter = getNodeConter(key);
            counter.increment();
        }

        MutableInt getNodeConter(String key) {
            Long hash = hashfunc.hash(key);
            if(!circle.containsKey(hash)) {
                SortedMap<Long, MutableInt> tailMap = circle.tailMap(hash);
                hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return circle.get(hash);
        }

        int[] getCounts() {
            final int[] counts = new int[counters.size()];
            int i = 0;
            for(MutableInt v : counters) {
                counts[i++] = v.getValue().intValue();
            }
            return counts;
        }

    }
}

package kmeans;

import gridool.GridJob;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.mapred.dht.task.DhtMapShuffleTask;
import gridool.mapred.dht.task.DhtReduceTask;
import gridool.mapred.dht.task.DhtScatterReduceTask;

import java.util.Collection;
import java.util.Iterator;

import xbird.util.lang.ObjectUtils;
import xbird.util.primitive.Primitives;

/**
 * Calculate the new means to be the centroid of the observations in the cluster.
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 */
public class Mstep {

    public static class MstepConf extends DhtMapReduceJobConf {

        private static final long serialVersionUID = 7809451918168086101L;
        private final int dimension;

        public MstepConf(String inputDhtName, int dimension) {
            super(inputDhtName);
            this.dimension = dimension;
        }

        @Override
        protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputDhtName, String destDhtName) {
            DhtMapShuffleTask task = new MstepMapTask(job, inputDhtName, destDhtName);
            return task;
        }

        @Override
        protected DhtReduceTask makeReduceTask(GridJob job, String inputDhtName, String destDhtName) {
            DhtReduceTask task = new MstepReduceTask(job, inputDhtName, destDhtName, dimension);
            return task;
        }

        private static class MstepMapTask extends DhtMapShuffleTask {

            private static final long serialVersionUID = -311719945937943498L;

            public MstepMapTask(GridJob job, String inputDhtName, String destDhtName) {
                super(job, inputDhtName, destDhtName, false);
            }
          
            /**
             * @param key 座標
             * @param value ラベル
             */
            @Override
            protected boolean process(byte[] key, byte[] value) {
                shuffle(value, key);
                return false;
            }

        }

        private static class MstepReduceTask extends DhtScatterReduceTask {
            private static final long serialVersionUID = -4217862919699049565L;
            private final int dimension;

            public MstepReduceTask(GridJob job, String inputDhtName, String destDhtName, int dimension) {
                super(job, inputDhtName, destDhtName, false);
                this.dimension = dimension;
            }

            @Override
            protected boolean process(byte[] key, Iterator<byte[]> values) {
                throw new IllegalStateException();
            }
            
            /**
             * @param key ラベル
             * @param values ラベルがおなじ座標群
             */
            @Override
            protected boolean process(byte[] key, Collection<byte[]> values) {
                double[] centroid = new double[dimension];
                assert (values.size() > 0);
                for(byte[] value : values) {
                    double[] vec = ObjectUtils.readObjectQuietly(value);
                    for(int i = 0; i < vec.length; ++i)
                        centroid[i] += vec[i];
                }

                int label = Primitives.getInt(key);
                System.err.print("label : " + label + " ");
                for(int i = 0; i < centroid.length; ++i) {
                    centroid[i] /= values.size();
                    System.err.print(centroid[i] + " ");
                }
                System.err.println("put!! " + this.getTaskId());
                collectOutput(key, ObjectUtils.toBytes(centroid));
                return false;
            }

        }
    }
}

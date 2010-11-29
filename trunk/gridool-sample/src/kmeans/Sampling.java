package kmeans;

import gridool.GridJob;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.mapred.dht.task.DhtMapShuffleTask;
import gridool.mapred.dht.task.DhtReduceTask;
import gridool.mapred.dht.task.DhtReducedMapShuffleTask;

import java.util.Collection;

import xbird.util.primitive.Primitives;

public class Sampling {

    public static class SamplingConf extends DhtMapReduceJobConf {

        private static final long serialVersionUID = 8916548873104407619L;
        private double rate;

        public SamplingConf(String inputDhtName, int showData, int size) {
            super(inputDhtName);
            this.rate = size > showData ? (double) showData / size : 1.0;
        }

        @Override
        protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputDhtName, String destDhtName) {
            return new SamplingMapTask(job, inputDhtName, destDhtName, false, rate);
        }

        @Override
        protected DhtReduceTask makeReduceTask(GridJob job, String inputDhtName, String destDhtName) {
            throw new IllegalStateException();
        }
    }

    private static class SamplingMapTask extends DhtReducedMapShuffleTask {
        private static final long serialVersionUID = -7388932414095245880L;

        private final double rate;

        public SamplingMapTask(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, double rate) {
            super(job, inputDhtName, destDhtName, removeInputDhtOnFinish);
            this.rate = rate;
        }

        @Override
        protected boolean process(byte[] key, Collection<byte[]> values) {
            final int size = values.size();
            for(int i = 0; i < size; i++) {
                if(Math.random() < rate) {
                    shuffle(Primitives.toBytes(1), key);
                }
            }
            return false;
        }

    }
}

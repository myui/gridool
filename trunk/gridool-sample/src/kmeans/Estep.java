package kmeans;

import gridool.GridClient;
import gridool.GridJob;
import gridool.directory.job.DirectoryGetJob;
import gridool.directory.ops.GetOperation;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.mapred.dht.task.DhtMapShuffleTask;
import gridool.mapred.dht.task.DhtReduceTask;

import java.rmi.RemoteException;

import xbird.util.lang.ObjectUtils;
import xbird.util.primitive.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 */
public class Estep {
    public static class EstepConf extends DhtMapReduceJobConf {
        private static final long serialVersionUID = -1552952354858102245L;

        private final String centroidDhtName;
        private final int K;
        private final double[][] centroids;

        public EstepConf(String inputDhtName, String centroidDhtName, int K) {
            super(inputDhtName);
            this.K = K;
            this.centroidDhtName = centroidDhtName;
            this.centroids = getCentroid();
        }

        private double[][] getCentroid() {
            final GridClient client = new GridClient();
            double[][] ret = new double[K][];
            for(int k = 0; k < K; ++k) {
                final GetOperation op = new GetOperation(centroidDhtName, Primitives.toBytes(k));
                byte[][] result = null;
                try {
                    result = client.execute(DirectoryGetJob.class, op);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                if(result.length == 0) {
                    throw new IllegalStateException("k not found: " + k);
                }
                ret[k] = ObjectUtils.readObjectQuietly(result[0]);
            }
            return ret;
        }

        @Override
        protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputDhtName, String destDhtName) {
            DhtMapShuffleTask task = new EstepMapTask(job, inputDhtName, destDhtName, K, centroids);
            return task;
        }

        @Override
        protected DhtReduceTask makeReduceTask(GridJob job, String inputDhtName, String destDhtName) {
            throw new IllegalStateException();
        }

        private static class EstepMapTask extends DhtMapShuffleTask {
            private static final long serialVersionUID = 1397678994490300198L;
            private final int K;
            private double[][] centroids;

            public EstepMapTask(GridJob job, String inputDhtName, String destDhtName, int k, double[][] centroids) {
                super(job, inputDhtName, destDhtName, false);
                this.K = k;
                this.centroids = centroids;
            }

            @Override
            protected boolean process(byte[] key, byte[] value) {
                final double[] data = ObjectUtils.readObjectQuietly(key);
                int id = 0;
                double min = Double.MAX_VALUE;
                for(int i = 0; i < K; ++i) {
                    double dist = distance(data, centroids[i]);
                    if(dist < min) {
                        id = i;
                        min = dist;
                    }
                }
                shuffle(key, Primitives.toBytes(id));
                return false;
            }

            // とりあえずユークリッド距離
            private double distance(double[] data, double[] centroid) {
                assert (data.length == centroid.length);
                double d = 0.0;
                for(int i = 0; i < data.length; i++)
                    d += Math.pow(data[i] - centroid[i], 2);
                return d;
            }
        }
    }
}

package kmeans;

import gridool.GridClient;
import gridool.GridJob;
import gridool.directory.job.DirectoryGetJob;
import gridool.directory.job.DirectoryMultiGetJob;
import gridool.directory.ops.GetOperation;
import gridool.directory.ops.MultiGetOperation;
import gridool.mapred.dht.DhtMapJob;
import gridool.mapred.dht.DhtMapReduceJob;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.mapred.dht.task.DhtMapShuffleTask;
import gridool.mapred.dht.task.DhtReduceTask;
import gridool.mapred.dht.task.DhtScatterReduceTask;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import kmeans.gui.KMeansListener;
import kmeans.gui.Sync;
import kmeans.util.DataLoader;
import kmeans.util.RandomDataGenerator;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;

import xbird.util.lang.ObjectUtils;
import xbird.util.primitive.Primitives;
import xbird.util.struct.ByteArray;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 */
public class KMeans implements Runnable {

    private Sync sync = null;
    private KMeansListener listener = null;

    @Option(name = "-k", usage = "number of cluster (default: 5)")
    private int K = 5;
    @Option(name = "-root-dir-path", usage = "data file\'s root directory path (default: null)")
    private String rootDirPath = null;
    @Option(name = "-suffix", usage = "data file\'s suffix (default: .dat)")
    private String suffix = ".dat";
    //@Option(name = "-input-data-dht-name", usage = "dht name of input data")
    //private String inputDataDhtName = null;
    @Option(name = "-random", usage = "use random data (default: false)")
    private boolean isRandomData = false;
    @Option(name = "-random-data-size", usage = "if use random data, then set size of random data set (default: 3000)")
    private int randomDataSize = 3000;
    @Option(name = "-max-iteration", usage = "upper bound of iteration (default: Integer.MAX_VALUE)")
    private int maxIteration = Integer.MAX_VALUE;
    @Option(name = "-random-data-dimension", usage = "if use random data, then set dimension of random data set (default: 2)")
    private int randomDataDimension = 2;
    @Option(name = "-h", usage = "show help")
    private boolean _showHelp = false;

    public static void main(String[] args) throws RemoteException {
        new KMeans().doMain(args);
    }

    private KMeans() {}

    public void doMain(String[] args) throws RemoteException {
        final CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            showHelp(parser);
            return;
        }
        if(_showHelp) {
            showHelp(parser);
            return;
        }
        if(rootDirPath == null) {
            isRandomData = true;
        }
        runKMeans();
    }

    private static void showHelp(CmdLineParser parser) {
        System.err.println("[Usage] \n $ java " + KMeans.class.getSimpleName()
                + parser.printExample(ExampleMode.ALL));
        parser.printUsage(System.err);
    }

    public KMeans(boolean isRadomData) {
        this.isRandomData = isRadomData;
    }

    public KMeans(boolean isRadomData, int randomDataSize) {
        this(isRadomData);
        this.randomDataSize = randomDataSize;
    }

    public KMeans(KMeansListener listener, String rootDirPath) {
        this.listener = listener;
        this.rootDirPath = rootDirPath;
    }

    public KMeans(int K, Sync sync, KMeansListener listener, String rootDirPath, String suffix) {
        this(listener, rootDirPath);
        this.K = K;
        this.sync = sync;
        this.suffix = suffix;
    }

    public KMeans(int K, KMeansListener listener, String rootDirPath, String suffix) {
        this(K, null, listener, rootDirPath, suffix);
    }

    public KMeans(int K, Sync sync, KMeansListener listener, boolean isRandomData) {
        this(isRandomData);
        this.K = K;
        this.sync = sync;
        this.listener = listener;
    }

    public KMeans(int K, Sync sync, KMeansListener listener, boolean isRandomData, int dataSize) {
        this(isRandomData, dataSize);
        this.K = K;
        this.sync = sync;
        this.listener = listener;
    }

    @Override
    public void run() {
        try {
            runKMeans();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public String runKMeans() throws RemoteException {
        System.out.println(" Begin " + (isRandomData ? "Generating Random Data" : "Loading Files"));
        final long dataBegin = System.nanoTime();
        double[][] dataSet = isRandomData ? RandomDataGenerator.uniformMultiDimension(randomDataSize, randomDataDimension)
                : DataLoader.loader(rootDirPath, suffix);
        final long dataEnd = System.nanoTime();
        System.out.println("    " + (isRandomData ? "Generating" : "Loading") + " Time: "
                + (dataEnd - dataBegin) / 1000000.0 + "ms");
        System.out.println("Finish " + (isRandomData ? "Generating Random Data" : "Loading Files"));

        String resultDataDhtName = null;
        while(true) {
            try {
                System.out.println(" Begin Initializing Data");
                final long initializeBegin = System.nanoTime();
                String inputDataDhtName = Initializer.dataInitializer(dataSet, "k-means", K);
                final long initializeEnd = System.nanoTime();
                System.out.println("    Initializing Time: " + (initializeEnd - initializeBegin)
                        / 1000000.0 + "ms");
                System.out.println("Finish Initializing Data");

                if(inputDataDhtName == null) {
                    throw new IllegalStateException("Returned input data dht name is null!!");
                }
                resultDataDhtName = calcKMeans(inputDataDhtName, dataSet[0].length, dataSet.length, maxIteration);
                outputResult(resultDataDhtName);
                break;
            } catch (NullPointerException e) {
                System.err.println("Invalid initial labels... So restart! ^^");
            }
        }
        return resultDataDhtName;
    }

    public String calcKMeans(final String inputDataDhtName, int dimension, int size, int maxIteration)
            throws RemoteException {
        final GridClient client = new GridClient();
        String preInputDataDhtName = inputDataDhtName;
        String preCentroidDhtName = null;

        client.deployClass(Estep.class);
        client.deployClass(Mstep.class);
        client.deployClass(Sampling.class);

        System.out.println("Begin Calcurating K-Means (K=" + K + ")\n");

        if(sync != null)
            drawDataToGUI(client, preInputDataDhtName, size, 0);

        final long start = System.nanoTime();
        for(int i = 1; i < maxIteration; i++) {
            System.out.printf("Iteration : %2d\n", i);

            System.out.println("     Begin Mstep");
            long mstepStart = System.nanoTime();
            Mstep.MstepConf mstepConf = new Mstep.MstepConf(preInputDataDhtName, dimension);
            String newCentroidDhtName = client.execute(DhtMapReduceJob.class.getName(), ObjectUtils.toBytes(mstepConf));
            final long mstepEnd = System.nanoTime();
            System.out.println("        Mstep Execution Time: " + (mstepEnd - mstepStart)
                    / 1000000.0 + "ms");
            System.out.println("    Finish Mstep\n");

            // GUIでストップされたらスタートされるまで処理を止める
            if(sync != null) {
                if(sync.waitForReleased()) {
                    System.err.println("    released!");
                }
            }
            if(sync != null)
                drawCentroidsToGUI(client, newCentroidDhtName, i);

            if(preCentroidDhtName != null
                    && convergenceCheck(newCentroidDhtName, preCentroidDhtName, K)) {
                System.out.println("    Iteration Execution Time: " + (mstepEnd - mstepStart)
                        / 1000000.0 + "ms\n");
                break;
            }

            System.out.println("    Begin Estep");
            final long estepStart = System.nanoTime();
            Estep.EstepConf estepConf = new Estep.EstepConf(preInputDataDhtName, newCentroidDhtName, K);
            String newInputDataDhtName = client.execute(DhtMapJob.class.getName(), ObjectUtils.toBytes(estepConf));
            final long estepEnd = System.nanoTime();
            System.out.println("        Estep Execution Time: " + (estepEnd - estepStart)
                    / 1000000.0 + "ms");
            System.out.println("    Finish Estep\n");

            // GUIでストップされたらスタートされるまで処理を止める
            if(sync != null) {
                if(sync.waitForReleased()) {
                    System.err.println("    released!");
                }
            }

            if(sync != null)
                drawDataToGUI(client, newInputDataDhtName, size, i);

            preInputDataDhtName = newInputDataDhtName;
            preCentroidDhtName = newCentroidDhtName;

            System.out.println("    Iteration Execution Time: "
                    + (estepEnd - estepStart + mstepEnd - mstepStart) / 1000000.0 + "ms\n");
        }
        long end = System.nanoTime();
        System.out.println("Finish Calcurating K-Means");
        System.out.println("Total Execution Time: " + (end - start) / 1000000.0 + "ms");

        return inputDataDhtName;
    }

    private void drawCentroidsToGUI(final GridClient client, String centroidDhtName, int itr)
            throws RemoteException {
        double[][] centroids = getCentorids(client, centroidDhtName);

        int[] labels = new int[K];
        for(int k = 0; k < K; k++)
            labels[k] = k;
        listener.onResponse(centroids, labels, K, true, itr);
    }

    private void drawDataToGUI(GridClient client, String inputDataDhtName, int datasize, int itr)
            throws RemoteException {
        Sampling.SamplingConf samplingConf = new Sampling.SamplingConf(inputDataDhtName, 500, datasize);
        String sampledKeysDhtName = client.execute(DhtMapJob.class.getName(), ObjectUtils.toBytes(samplingConf));

        GetOperation inputDataGet = new GetOperation(sampledKeysDhtName, Primitives.toBytes(1));
        final byte[][] sampledKeys = client.execute(DirectoryGetJob.class.getName(), inputDataGet);
        final int sampleSize = sampledKeys.length;
        final MultiGetOperation getOp = new MultiGetOperation(inputDataDhtName, sampledKeys);
        HashMap<ByteArray, ArrayList<byte[]>> sampledLabelMap = client.execute(DirectoryMultiGetJob.class, getOp);

        double[][] sampledPositions = new double[sampleSize][];
        int[] sampledLabels = new int[sampleSize];
        for(int j = 0; j < sampleSize; j++) {
            sampledPositions[j] = ObjectUtils.readObjectQuietly(sampledKeys[j]);

            ArrayList<byte[]> list = sampledLabelMap.get(new ByteArray(sampledKeys[j]));
            byte[] b = list.get(0);
            sampledLabels[j] = Primitives.getInt(b);
        }

        listener.onResponse(sampledPositions, sampledLabels, K, false, itr);
    }

    private double[][] getCentorids(GridClient client, String centroidDhtName)
            throws RemoteException {
        //		client.deployClass(GetAllCentroids.class);
        //    	ArrayList<Pair<Integer, double[]>> centroids = client.execute(GetAllCentroids.class.getName(), centroidDhtName);
        //		
        //    	if (centroids.size() != K) {
        //    		for (Pair<Integer, double[]> centroid: centroids) {
        //        		System.err.print(centroid.first + " : ");
        //        		for (double d: centroid.second) {
        //            		System.err.print(d + " ");
        //        		}
        //        		System.err.println();
        //        	}
        //    		throw new IllegalStateException("Cannot get all centroids at KMeans!!");
        //    	}
        //    	
        double[][] ret = new double[K][];
        //    	for (Pair<Integer, double[]> centroid: centroids) ret[centroid.first] = centroid.second;
        for(int k = 0; k < K; ++k) {
            final GetOperation op = new GetOperation(centroidDhtName, Primitives.toBytes(k));
            byte[][] result = client.execute(DirectoryGetJob.class, op);
            //System.out.printf("%2d: %d", k, Primitives.getInt(result[0]));
            ret[k] = ObjectUtils.readObjectQuietly(result[0]);
        }

        return ret;
    }

    private void outputResult(String inputDhtName) throws RemoteException {
        final GridClient client = new GridClient();
        client.deployClass(CreateHistogramConf.class);
        CreateHistogramConf histogramConf = new CreateHistogramConf(inputDhtName);
        final String histogramDhtName = client.execute(DhtMapReduceJob.class.getName(), ObjectUtils.toBytes(histogramConf));

        for(int k = 0; k < K; ++k) {
            final GetOperation op = new GetOperation(histogramDhtName, Primitives.toBytes(k));
            byte[][] result = null;
            try {
                result = client.execute(DirectoryGetJob.class, op);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            System.out.printf("%2d: %d", k, Primitives.getInt(result[0]));
        }
    }

    public static class CreateHistogramConf extends DhtMapReduceJobConf {

        private static final long serialVersionUID = -3317746833916779221L;

        public CreateHistogramConf(String inputDhtName) {
            super(inputDhtName);
        }

        @Override
        protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputDhtName, String destDhtName) {
            DhtMapShuffleTask task = new CreateHistogramMapTask(job, inputDhtName, destDhtName);
            return task;
        }

        @Override
        protected DhtReduceTask makeReduceTask(GridJob job, String inputDhtName, String destDhtName) {
            DhtReduceTask task = new CreateHistogramReduceTask(job, inputDhtName, destDhtName);
            return task;
        }

        private static class CreateHistogramMapTask extends DhtMapShuffleTask {

            private static final long serialVersionUID = 6218750792527735032L;

            public CreateHistogramMapTask(GridJob job, String inputDhtName, String destDhtName) {
                super(job, inputDhtName, destDhtName, false);
            }

            @Override
            protected boolean process(byte[] key, byte[] value) {
                shuffle(value, key);
                return false;
            }
        }

        private static class CreateHistogramReduceTask extends DhtScatterReduceTask {

            private static final long serialVersionUID = 3728462703401021409L;

            public CreateHistogramReduceTask(GridJob job, String inputDhtName, String destDhtName) {
                super(job, inputDhtName, destDhtName, false);
            }

            @Override
            protected boolean process(byte[] key, Iterator<byte[]> values) {
                throw new IllegalStateException();
            }

            @Override
            protected boolean process(byte[] key, Collection<byte[]> values) {
                collectOutput(key, Primitives.toBytes(values.size()));
                return false;
            }
        }
    }

    // 収束判定
    private static boolean convergenceCheck(String preCentroidDhtName, String newCentroidDhtName, int K) {
        final GridClient client = new GridClient();
        byte[][] ids = new byte[K][];
        for(int k = 0; k < K; ++k)
            ids[k] = Primitives.toBytes(k);

        MultiGetOperation preOps = new MultiGetOperation(preCentroidDhtName, ids);
        MultiGetOperation newOps = new MultiGetOperation(newCentroidDhtName, ids);
        HashMap<ByteArray, ArrayList<byte[]>> preMap = null;
        HashMap<ByteArray, ArrayList<byte[]>> newMap = null;
        try {
            preMap = client.execute(DirectoryMultiGetJob.class, preOps);
            newMap = client.execute(DirectoryMultiGetJob.class, newOps);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        if(!equals(preMap, newMap, 1e-9, K))
            return false;

        return true;
    }

    private static boolean equals(HashMap<ByteArray, ArrayList<byte[]>> lhs, HashMap<ByteArray, ArrayList<byte[]>> rhs, double eps, int K) {
        double total_error = 0;

        for(int k = 0; k < K; ++k) {
            final ByteArray key = new ByteArray(Primitives.toBytes(k));
            ArrayList<byte[]> llist = lhs.get(key);
            ArrayList<byte[]> rlist = rhs.get(key);

            if(llist == null || rlist == null) {
                throw new IllegalStateException("k not found: " + k);
            }

            double[] preCentroid = ObjectUtils.readObjectQuietly(llist.get(0));
            double[] newCentroid = ObjectUtils.readObjectQuietly(rlist.get(0));

            for(int i = 0; i < preCentroid.length; ++i)
                total_error += Math.pow(preCentroid[i] - newCentroid[i], 2);
        }

        System.out.println("    square of total travel distance : " + total_error + "\n");
        return total_error < eps;
    }
}

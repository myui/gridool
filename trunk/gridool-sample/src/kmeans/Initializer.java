package kmeans;

import gridool.GridClient;
import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.directory.helpers.DirectoryTaskAdapter;
import gridool.directory.job.DirectoryAddJob;
import gridool.directory.ops.AddOperation;
import gridool.routing.GridRouter;
import gridool.util.GridUtils;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;

import kmeans.util.Triad;
import xbird.util.lang.ObjectUtils;
import xbird.util.primitive.Primitives;

public class Initializer {
    public static String getDataInfo(double[][] data, int K) throws RemoteException {
        System.out.println("Begin getDataInfo");
        final GridClient client = new GridClient();
        final String dataDhtName = "K-Means#" + System.nanoTime();
        client.deployClass(ReadFileJob.class);
        client.deployClass(Initializer.class);
        client.deployClass(Triad.class);
        System.out.println("Finish getDataInfo");

        return dataDhtName;
    }

    public static final class ReadFileJob extends GridJobBase<byte[], Integer> {

        private static final long serialVersionUID = 8663433775293728257L;
        private int dimension = 0;

        public ReadFileJob() {
            super();
        }

        @Override
        public Map<GridTask, GridNode> map(GridRouter router, byte[] barg) throws GridException {
            final Triad<String, double[][], Integer> arg = ObjectUtils.readObjectQuietly(barg, Triad.class.getClassLoader());
            final String dataDhtName = arg.first;
            final double[][] data = arg.second;
            final int K = arg.third;

            if(data == null)
                return Collections.emptyMap();

            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(data.length);
            final Random rand = new Random(System.nanoTime());

            final AddOperation ops = new AddOperation(dataDhtName);
            boolean[] appearance = new boolean[K];
            for(double[] point : data) {
                int k = rand.nextInt(K);
                appearance[k] = true;
                ops.addMapping(ObjectUtils.toBytes(point), Primitives.toBytes(k));
            }

            for(int i = 0; i < K; i++) {
                if(!appearance[i]) {
                    throw new GridException("k not found: " + i);
                }
            }

            dimension = data[0].length;
            GridTask task = new DirectoryTaskAdapter(this, ops);
            GridNode node = router.selectNode(GridUtils.getTaskKey(task));
            map.put(task, node);

            return map;
        }

        @Override
        public Integer reduce() throws GridException {
            return dimension;
        }

        @Override
        public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
            return GridTaskResultPolicy.CONTINUE;
        }

    }

    public static String dataInitializer(double[][] data, String algorithm, int K)
            throws RemoteException {
        if(data == null)
            return null;
        if(data[0] == null)
            return null;

        final int size = data.length;
        final GridClient client = new GridClient();
        final String dataDhtName = algorithm + "#" + System.nanoTime();
        final Random rand = new Random(System.nanoTime());

        final AddOperation ops = new AddOperation(dataDhtName);
        for(int i = 0; i < size; i++) {
            ops.addMapping(ObjectUtils.toBytes(data[i]), Primitives.toBytes(rand.nextInt(K)));
        }
        client.execute(DirectoryAddJob.class, ops);

        return dataDhtName;
    }
}

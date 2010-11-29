package kmeans;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridDirectoryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.directory.ILocalDirectory;
import gridool.routing.GridRouter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;

import xbird.storage.DbException;
import xbird.storage.index.CallbackHandler;
import xbird.storage.index.Value;
import xbird.storage.indexer.BasicIndexQuery;
import xbird.util.lang.ObjectUtils;
import xbird.util.primitive.Primitives;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 * @author Naoyoshi Aikawa 
 */
public class GetAllCentroids extends GridJobBase<String, ArrayList<Pair<Integer, double[]>>> {
    private static final long serialVersionUID = -5958188465961032014L;
    private ArrayList<Pair<Integer, double[]>> centroids = new ArrayList<Pair<Integer, double[]>>();

    @Override
    public Map<GridTask, GridNode> map(GridRouter router, String arg) throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        for(GridNode node : nodes) {
            System.err.println(node.getKey());
        }
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GetAllCentroidsTask task = new GetAllCentroidsTask(this, arg);
            map.put(task, node);
        }
        return map;
    }

    @Override
    public ArrayList<Pair<Integer, double[]>> reduce() throws GridException {
        return centroids;
    }

    @SuppressWarnings("unchecked")
    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        centroids.addAll((ArrayList<Pair<Integer, double[]>>) result.getResult());//(Double)result.getResult();
        return GridTaskResultPolicy.CONTINUE;
    }

    private final class GetAllCentroidsTask extends GridTaskAdapter {
        private static final long serialVersionUID = 3022628740163516762L;

        private final String inputDhtName;

        @GridDirectoryResource
        private transient ILocalDirectory directory;

        @Override
        public boolean injectResources() {
            return true;
        }

        @SuppressWarnings("unchecked")
        public GetAllCentroidsTask(GridJob job, String inputDhtName) {
            super(job, false);
            this.inputDhtName = inputDhtName;
        }

        @Override
        public Serializable execute() throws GridException {
            GetAllCentroidsCallbackHandler callback = new GetAllCentroidsCallbackHandler();
            try {
                directory.retrieve(inputDhtName, new BasicIndexQuery.IndexConditionANY(), callback);
            } catch (DbException e) {
                throw new GridException(e);
            }
            return callback.getPartialCentroids();
        }
    }

    private final class GetAllCentroidsCallbackHandler implements CallbackHandler {
        private ArrayList<Pair<Integer, double[]>> paratial_centroids = new ArrayList<Pair<Integer, double[]>>();

        @Override
        public boolean indexInfo(Value arg0, long arg1) {
            throw new IllegalStateException();
        }

        @Override
        public boolean indexInfo(Value arg0, byte[] arg1) {
            double[] centroid = ObjectUtils.readObjectQuietly(arg1);
            paratial_centroids.add(new Pair<Integer, double[]>(Primitives.getInt(arg0.getData()), centroid));
            return false;
        }

        public ArrayList<Pair<Integer, double[]>> getPartialCentroids() {
            return paratial_centroids;
        }

    }
}
package kmeans.gui;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 */
public interface KMeansListener {

    public void onResponse(double[][] positions, int[] labels, int K, boolean isCentroid, int iteration);

}
package kmeans.gui;

public interface KMeansListener {

    public void onResponse(double[][] positions, int[] labels, int K, boolean isCentroid, int iteration);

}
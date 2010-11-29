package kmeans.util;

import java.util.Random;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Kohsuke Morimoto
 * @author Naoyoshi Aikawa
 */
public class RandomDataGenerator {

    public static double[][] uniformMultiDimension(int size, int dimension) {
        final Random rand = new Random(System.currentTimeMillis() + System.nanoTime());

        double[][] ret = new double[size][dimension];
        for(int i = 0; i < size; ++i)
            for(int j = 0; j < dimension; ++j)
                ret[i][j] = rand.nextDouble();

        return ret;
    }

    public static double[][] uniform2D(int size) {
        return uniformMultiDimension(size, 2);
    }

    public static double[][] uniform3D(int size) {
        return uniformMultiDimension(size, 3);
    }

    public static double[][] zipfMultiDimension(int size, double alpha, int n, int dimension) {
        final Zipf z = new Zipf(n, alpha);

        double[][] ret = new double[size][dimension];
        for(int i = 0; i < size; ++i)
            for(int j = 0; j < dimension; ++j)
                ret[i][j] = z.next();

        return ret;
    }

    public static double[][] zipf1D(int size, double alpha, int n) {
        return zipfMultiDimension(size, alpha, n, 1);
    }

    public static double[][] zipf2D(int size, double alpha, int n) {
        return zipfMultiDimension(size, alpha, n, 2);
    }

    public static double[][] zipf3D(int size, double alpha, int n) {
        return zipfMultiDimension(size, alpha, n, 3);
    }

    private static class Zipf {

        private double c = 0;
        private int n;
        private double alpha;

        public Zipf(int n, double alpha) {
            this.n = n;
            this.alpha = alpha;
            for(int i = 1; i <= n; ++i)
                c = c + (1.0 / Math.pow((double) i, alpha));
            c = 1.0 / c;
        }

        public double next() {
            double z;
            final Random rand = new Random(System.currentTimeMillis() + System.nanoTime());
            do {
                z = rand.nextDouble();
            } while(z == 0 || z == 1);

            double sum_prob = 0;
            double zipf_value = 0;
            for(int i = 1; i <= n; ++i) {
                sum_prob = sum_prob + c / Math.pow((double) i, alpha);
                if(sum_prob >= z) {
                    zipf_value = i;
                    break;
                }
            }
            assert ((zipf_value >= 1) && (zipf_value <= n));

            return zipf_value;
        }
    }
}

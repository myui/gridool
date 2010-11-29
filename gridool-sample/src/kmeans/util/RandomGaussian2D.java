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
public class RandomGaussian2D {
    private final Random rand;
    private final double[] mu;
    private final double[][] sigma;
    private double x, y;

    public RandomGaussian2D(double[] mu, double[][] sigma, long seed) {
        this.mu = mu;
        this.sigma = sigma;
        if(mu.length != 2 || sigma.length != 2 || sigma[0].length != 2
                || sigma[0][1] != sigma[1][0]) {
            throw new IllegalStateException("Illegal Gaussian2D Paramaters");
        }
        this.x = mu[0];
        this.y = mu[1];
        rand = new Random(seed);
    }

    public RandomGaussian2D(double[] mu, double[][] sigma) {
        this(mu, sigma, System.nanoTime());
    }

    public RandomGaussian2D() {
        this(new double[] { 0.0, 0.0 }, new double[][] { { 1.0, 0.0 }, { 0.0, 1.0 } });
    }

    public double[] next() {
        x = rand.nextGaussian();
        x *= sigma[0][0] - sigma[0][1] * sigma[1][0] / sigma[1][1];
        x += mu[0] + (y - mu[1]) * sigma[0][1] / sigma[1][1];
        y = rand.nextGaussian();
        y *= sigma[1][1] - sigma[1][0] * sigma[0][1] / sigma[0][0];
        y += mu[1] + (x - mu[0]) * sigma[1][0] / sigma[0][0];
        return new double[] { x, y };
    }

    public static void main(String[] args) { //TEST
        RandomGaussian2D g = new RandomGaussian2D(new double[] { 2.0, 10.0 }, new double[][] {
                { 2.0, 0.0 }, { 0.0, 2.0 } });
        for(int i = 0; i < 20; i++) {
            double xy[] = g.next();
            System.out.println(xy[0] + "," + xy[1]);
        }
    }
}

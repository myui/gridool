package kmeans.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

import kmeans.KMeans;
import kmeans.util.RandomDataGenerator;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;

public class KMeansGUI implements KMeansListener {

    private ChartPanel cPanel;
    private JFrame window;
    private JLabel itrLabel;

    public KMeansGUI() {}

    @Option(name = "-k", usage = "number of cluster")
    private int K = 5;
    @Option(name = "-root-dir-path", usage = "data file\'s root directory path")
    private String rootDirPath = null;
    @Option(name = "-suffix", usage = "data file\'s suffix (default: .dat)")
    private String suffix = ".dat";
    @Option(name = "-random", usage = "use random data (default: false)")
    private boolean isRandomData = false;
    @Option(name = "-data-size", usage = "if use random data, then set size of random data set (default: 3000)")
    private int dataSize = 3000;
    //@Option(name = "-output", usage = "output png file each iteration (default: false)")
    //private boolean isOutput = false;
    @Option(name = "-h", usage = "show help")
    private boolean _showHelp = false;

    /**
     * XYScatterChartの作成と使用.
     */
    public void workXYScatterChart(final Sync sync) {
        // まずXYScatterChartを作成する.
        JFreeChart xyScatterChart = getXYScatterChart(createRandomXYDataset(0, 0));

        // // 作成したXYScatterChartでPNGファイルを作成.
        // File outFile = new File("./xyscatterchart.png");
        // try {
        // ChartUtilities.saveChartAsPNG(outFile, xyScatterChart, 500, 500);
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new GridLayout(1, 2));
        // buttonPanel.setLayout(new FlowLayout());

        final JButton startButton = new JButton("Start");
        final JButton stopButton = new JButton("Stop");
        stopButton.setEnabled(false);
        startButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                startButton.setEnabled(false);
                stopButton.setEnabled(true);
                sync.start();
            }
        });
        stopButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                startButton.setEnabled(true);
                stopButton.setEnabled(false);
                sync.stop();
            }
        });
        buttonPanel.add(startButton);
        buttonPanel.add(stopButton);

        itrLabel = new JLabel("Iteration 0", SwingConstants.CENTER);
        itrLabel.setBackground(Color.white);
        itrLabel.setOpaque(true);
        itrLabel.setFont(new Font("SunsSerif", Font.BOLD, 14));

        // Windowを作成.
        cPanel = new ChartPanel(xyScatterChart);
        window = new JFrame("KMeans Plot");
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        window.setBounds(10, 10, 500, 500);
        window.getContentPane().add(itrLabel, BorderLayout.CENTER);
        window.getContentPane().add(cPanel, BorderLayout.NORTH);
        window.getContentPane().add(buttonPanel, BorderLayout.SOUTH);
        window.setVisible(true);
    }

    /**
     * JFreeChartオブジェクトの作成
     */
    public JFreeChart getXYScatterChart(XYDataset xyData) {
        // XYDatasetをデータにしてJFreeChartを作成
        JFreeChart xyScatterChart = ChartFactory.createScatterPlot("k-means Demo", "", "", xyData, PlotOrientation.VERTICAL, true, true, true);
        configXYScatterChart(xyScatterChart);
        return xyScatterChart;
    }

    public XYDataset createXYDatasetFromKMeansData(double[][] positions, int[] labels, int K) {
        final int dataSize = positions.length;
        assert (dataSize == labels.length);
        final int dimension = positions[0].length;
        assert (dimension >= 2);
        XYSeriesCollection xySeriesCollection = new XYSeriesCollection();
        ArrayList<ArrayList<Double>> plotXtmp = new ArrayList<ArrayList<Double>>();
        ArrayList<ArrayList<Double>> plotYtmp = new ArrayList<ArrayList<Double>>();
        for(int k = 0; k < K; k++)
            plotXtmp.add(new ArrayList<Double>());
        for(int k = 0; k < K; k++)
            plotYtmp.add(new ArrayList<Double>());

        for(int i = 0; i < dataSize; i++) {
            plotXtmp.get(labels[i]).add(positions[i][0]);
            plotYtmp.get(labels[i]).add(positions[i][1]);
        }

        double[][] plotX = new double[K][];
        double[][] plotY = new double[K][];

        for(int k = 0; k < K; k++) {
            final int len = plotXtmp.get(k).size();
            plotX[k] = new double[len];
            plotY[k] = new double[len];
            for(int i = 0; i < len; i++) {
                plotX[k][i] = plotXtmp.get(k).get(i);
                plotY[k][i] = plotYtmp.get(k).get(i);
            }
        }

        for(int series = 0; series < K; series++) {
            XYSeries xySeries = new XYSeries("Series " + series);
            for(int item = 0; item < plotX[series].length; item++) {
                xySeries.add(plotX[series][item], plotY[series][item]);
            }
            xySeriesCollection.addSeries(xySeries);
        }
        return xySeriesCollection;
    }

    /**
     * XYDatasetのオブジェクト作成
     */
    public XYDataset createRandomXYDataset(int numOfDataType, int dataSize) {
        XYSeriesCollection xySeriesCollection = new XYSeriesCollection();
        double[][][] data = new double[numOfDataType][][];
        for(int k = 0; k < numOfDataType; k++) {
            data[k] = RandomDataGenerator.uniform2D(dataSize);
        }

        for(int series = 0; series < numOfDataType; series++) {
            XYSeries xySeries = new XYSeries("Series " + series);
            for(int item = 0; item < dataSize; item++) {
                xySeries.add(data[series][item][0], data[series][item][1]);
            }
            xySeriesCollection.addSeries(xySeries);
        }
        return xySeriesCollection;
    }

    /**
     * JFreeChartの設定
     */
    public void configXYScatterChart(JFreeChart xyScatterChart) {
        XYPlot xyPlot = xyScatterChart.getXYPlot();
        /* カーソル位置で横方向の補助線をいれる */
        xyPlot.setDomainCrosshairVisible(true);
        /* カーソル位置で縦方向の補助線をいれる */
        xyPlot.setRangeCrosshairVisible(true);

        /* 横軸の設定 */
        NumberAxis xAxis = (NumberAxis) xyPlot.getDomainAxis();
        xAxis.setAutoRange(true);

        /* 縦軸の設定 */
        NumberAxis yAxis = (NumberAxis) xyPlot.getRangeAxis();
        yAxis.setAutoRange(true);
    }

    public static void main(String args[]) {
        new KMeansGUI().doMain(args);
    }

    private void doMain(String[] args) {
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

        Sync sync = new Sync();
        workXYScatterChart(sync);
        KMeans kmeans = isRandomData ? new KMeans(K, sync, this, isRandomData, dataSize)
                : new KMeans(K, sync, this, rootDirPath, suffix);
        new Thread(kmeans).start();
    }

    private static void showHelp(CmdLineParser parser) {
        System.err.println("[Usage] \n $ java " + KMeansGUI.class.getSimpleName()
                + parser.printExample(ExampleMode.ALL));
        parser.printUsage(System.err);
    }

    @Override
    public void onResponse(double[][] positions, int[] labels, int K, boolean isCentroid, int itr) {
        // FIXME 誰かもっといい方法でChartの更新を実装して＞＜
        window.getContentPane().remove(cPanel);
        if(isCentroid) {
            // cPanel = new　ChartPanel(getXYScatterChart(createXYDatasetFromKMeansData(positions,　labels, K)));
        } else {
            cPanel = new ChartPanel(getXYScatterChart(createXYDatasetFromKMeansData(positions, labels, K)));
            itrLabel.setText("Iteration " + itr);
        }
        window.getContentPane().add(cPanel, BorderLayout.NORTH);
        window.validate();
        /*
        File outFile = new File("./kmeans" + itr + ".png");
        try {
        	 ChartUtilities.saveChartAsPNG(outFile, xyScatterChart, 500, 500);//森本さんマジTODO
        } catch (IOException e) {
        	e.printStackTrace();
        }
        */
    }

}

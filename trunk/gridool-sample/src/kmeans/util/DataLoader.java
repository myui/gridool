package kmeans.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import xbird.util.io.FileUtils;

public class DataLoader {
    public static double[][] loader(String rootDirPath, String suffix) {
        File root = new File(rootDirPath);
        List<File> files = FileUtils.listFiles(root, suffix, true);
        ArrayList<double[]> data = new ArrayList<double[]>();
        for(File file : files) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while((line = br.readLine()) != null) {
                    final StringTokenizer tokenizer = new StringTokenizer(line, " ");
                    double[] point = new double[tokenizer.countTokens()];
                    for(int i = 0; i < point.length; ++i) {
                        point[i] = Double.parseDouble(tokenizer.nextToken());
                    }
                    data.add(point);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        double[][] ary = new double[data.size()][];
        return data.toArray(ary);
    }
}

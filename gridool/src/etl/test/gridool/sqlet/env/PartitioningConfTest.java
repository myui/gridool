package gridool.sqlet.env;

import gridool.sqlet.SqletException;
import gridool.sqlet.catalog.PartitioningConf;

import org.junit.Test;

public class PartitioningConfTest {

    @Test
    public void testLoadSettingsString() throws SqletException {
        PartitioningConf conf = new PartitioningConf();
        conf.loadSettings("/home/myui/workspace/gridool/src/etl/test/gridool/sqlet/env/partitioning_test01.csv");
        conf.toString();
    }

}

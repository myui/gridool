package gridool.sqlet.partitioning;

import gridool.sqlet.SqletException;

import org.junit.Test;

public class PartitioningConfTest {

    @Test
    public void testLoadSettingsString() throws SqletException {
        PartitioningConf conf = new PartitioningConf();
        conf.loadSettings("/home/myui/workspace/gridool/src/etl/test/gridool/sqlet/partitioning/partitioning_test01.csv");
        conf.toString();
    }

}

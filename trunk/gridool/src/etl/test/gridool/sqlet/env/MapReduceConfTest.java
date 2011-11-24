package gridool.sqlet.env;

import gridool.sqlet.SqletException;
import gridool.sqlet.catalog.MapReduceConf;
import gridool.sqlet.catalog.MapReduceConf.Reducer;

import java.util.List;

import org.junit.Test;

public class MapReduceConfTest {

    @Test
    public void testLoadReducers() throws SqletException {
        MapReduceConf conf = new MapReduceConf();
        conf.loadReducers("/home/myui/workspace/gridool/src/etl/test/gridool/sqlet/env/reducers01.csv");
        List<Reducer> reducers = conf.getReducers(true);
        System.out.println(reducers);
    }

}

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import junit.framework.Assert;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

public class SpymemcachedClientTest {

    public static void main(String[] args) throws IOException, InterruptedException,
            ExecutionException {
        MemcachedClient memc = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses("192.168.142.252:11211"));
        Assert.assertTrue(memc.set("some_key", 0, "this is a value").get());
        System.out.println(memc.get("some_key"));
        System.out.println(memc.get("some_key"));
        memc.shutdown();
    }

}

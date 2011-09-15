import junit.framework.Assert;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

public class JavaMemcachedClientTest {

    public static void main(String[] args) {
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(new String[] { "192.168.142.132:11211" });
        pool.initialize();

        MemCachedClient mcc = new MemCachedClient(/* tcp */true, /* binary */true);
        mcc.setPrimitiveAsString(true);
        
        boolean stat = mcc.set("name", "My name is foo");
        Assert.assertTrue(stat);
        
        String name = (String) mcc.get("name");
        System.out.println(name);
        
        name = (String) mcc.get("name");
        System.out.println(name);
    }

}

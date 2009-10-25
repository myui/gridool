package gridool.memcached.ops;

import java.io.Serializable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class KeyValueOperation<T extends Serializable> implements MemcachedOperation {
    private static final long serialVersionUID = -7391418079257697465L;

    private final String key;
    private final T value;
    private final boolean async;
    private int replicas = 0;

    public KeyValueOperation(String key, T value, boolean async) {
        this.key = key;
        this.value = value;
        this.async = async;
    }

    @Override
    public boolean isAsyncOps() {
        return async;
    }

    @Override
    public boolean isReadOnlyOps() {
        return false;
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

    @Override
    public int getNumberOfReplicas() {
        return replicas;
    }

    @Override
    public void setNumberOfReplicas(int replicas) {
        this.replicas = replicas;
    }

}
package kmeans.gui;

import java.util.concurrent.atomic.AtomicBoolean;

public class Sync {

    private final AtomicBoolean locked;

    public Sync(boolean locked) {
        this.locked = new AtomicBoolean(locked);
    }

    public Sync() {
        this(true);
    }

    public void start() {
        if(locked.getAndSet(false) == true) {
            synchronized(locked) {
                locked.notifyAll();
            }
        }
    }

    public boolean waitForReleased() {
        if(locked.get() == true) {
            synchronized(locked) {
                try {
                    locked.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }
        return false;
    }

    public void stop() {
        locked.set(true);
    }

}

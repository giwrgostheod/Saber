package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

public class SynchronizedFlag {
	
    private boolean flag;
    private final Object lock = new Object();

    public SynchronizedFlag(boolean value) {
        this.flag = new Boolean(value);
    }

    public void reverseFlag() throws InterruptedException {
        synchronized(lock) {
            flag = !flag;
            lock.notifyAll();
        }
    }
    
    public void setCount(boolean value) {
        synchronized(lock) {
            flag = new Boolean(value);
            lock.notifyAll();
        }
    }

    public boolean getFlag() {
        synchronized(lock) {
            return (boolean) flag;
        }
    }
}
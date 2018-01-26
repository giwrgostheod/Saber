package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.NewGenerator;

public class SynchronizedFlag1 {
	
    private int flag;
    private final Object lock = new Object();

    public SynchronizedFlag1(int value) {
        this.flag = new Integer(value);
    }

    public void reverseFlag() throws InterruptedException {
        synchronized(lock) {
            flag --;
            lock.notifyAll();
        }
    }
    
    public void subtractFlag() throws InterruptedException {
        synchronized(lock) {
            flag --;
            lock.notifyAll();
        }
    }
    
    
    public void incrementFlag() throws InterruptedException {
        synchronized(lock) {
            flag ++;
            lock.notifyAll();
        }
    }
    
    public void setCount(int value) {
        synchronized(lock) {
            flag = new Integer(value);
            lock.notifyAll();
        }
    }

    public int getFlag() {
        synchronized(lock) {
            return (int) flag;
        }
    }
}
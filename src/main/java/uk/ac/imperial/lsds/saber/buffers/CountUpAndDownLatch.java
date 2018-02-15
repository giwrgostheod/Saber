package uk.ac.imperial.lsds.saber.buffers;

import java.util.concurrent.CountDownLatch;

public class CountUpAndDownLatch {
	
    private CountDownLatch latch;
    private final Object lock = new Object();

    public CountUpAndDownLatch(int count) {
        this.latch = new CountDownLatch(count);
    }

    public void countDownOrWaitIfZero() throws InterruptedException {
        synchronized(lock) {
            while(latch.getCount() == 0) {
                lock.wait();
            }
            latch.countDown();
            lock.notifyAll();
        }
    }

    public void waitUntilZero() throws InterruptedException {
        synchronized(lock) {
            while(latch.getCount() != 0) {
                lock.wait();
            }
        }
    }

    public void countUp() {
        synchronized(lock) {
            latch = new CountDownLatch((int) latch.getCount() + 1);
            lock.notifyAll();
        }
    }
    
    public void setCount(int count) {
        synchronized(lock) {
            latch = new CountDownLatch(count);
            lock.notifyAll();
        }
    }

    public int getCount() {
        synchronized(lock) {
            return (int) latch.getCount();
        }
    }
}
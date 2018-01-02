package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.devices.TheCPU;

public class EventGenerator implements Runnable{
	
	private final int numberOfThreads;
	private final ITupleSchema inputSchema;
	private final int adsPerCampaign;
	private final long[][] ads;
	private final int coreToBind;
	private final SynchronizedFlag flag;
	private List<List<Integer>> positionsList;
	
	private AtomicInteger latch;
	private volatile BufferQueue buffer;
	private volatile long timestamp;
		
	public EventGenerator (BufferQueue buffer, int adsPerCampaign, long[][] ads, int coreToBind, SynchronizedFlag flag) {
		this.buffer = buffer;
		this.numberOfThreads = buffer.getNumberOfThreads();
		this.inputSchema = buffer.getInputSchema();
		this.adsPerCampaign = adsPerCampaign;
		this.ads = ads;
		this.coreToBind = coreToBind;
		this.flag = flag;
    	this.positionsList = buffer.getPositionsList();
		this.latch = new AtomicInteger();
		this.latch.set(numberOfThreads);
		timestamp = 0;
	}
	
    public void run(){
    	
    	TheCPU.getInstance().bind(this.coreToBind);
		System.out.println(String.format("[DBG] bind Event Generator thread to core %2d", this.coreToBind));
    	
    	int i;
    	Thread worker;    	
        for (i = 0; i < numberOfThreads; i++) {
        	worker = new Thread(new WorkerThread(this, positionsList.get(i), this.coreToBind + i + 1));
        	worker.start();
        }        
                
        long remaining;
        long start = System.nanoTime();
        int counter = 0;
        while (true) {
        	
        	while (buffer.getBuffer().getThreadsLatch().getCount() != 0)
        		Thread.yield();
        	
/*        	while (buffer.getBuffer().isReady().get())
        		Thread.yield();*/
        	
    		while (flag.getFlag())
    			Thread.yield();
        	
			//System.out.println("Entered with " +buffer.getBuffer().getThreadsLatch().getCount());
			
/*			if (buffer.getBuffer().getReadLatch().getCount() == 0) { // isRead().get() == 0) {
				buffer.getBuffer().getReadLatch().setCount(buffer.getBuffer().getTimestampIterations());//isRead().set(buffer.getBuffer().getTimestampIterations());
				timestamp++;
				System.out.println("The timestamp is " + timestamp);
			}*/
			
			if (counter == buffer.getBuffer().getTimestampIterations() -1) { // isRead().get() == 0) {
				timestamp += 1;
				counter = 0;
				//System.out.println("The timestamp is " + timestamp);
			}
			
        	buffer.getBuffer().isReady().set(true);
    		try {
				flag.reverseFlag();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

        	remaining = start + 1000000 - System.nanoTime();
/*			if (remaining < 0) {
				System.err.println("The computation is slower than required");
				System.exit(1);
			}*/
//        	if (remaining > 0)
//        		LockSupport.parkNanos(remaining);
        	

        	start = System.nanoTime();
        	
        	//latch.set(numberOfThreads);
        	buffer.getBuffer().getThreadsLatch().setCount(numberOfThreads);
			//System.out.println("Reset latch: " +buffer.getBuffer().getThreadsLatch().getCount());
			counter++;
        }
        
     }    
	
	public ITupleSchema getInputSchema() {
		
		return this.inputSchema;
	};
	
	public int getAdsPerCampaign () {
		return this.adsPerCampaign;
	};
	
	public long [][] getAds () {
		
		return this.ads;
	};
	
	public BufferQueue getBufferQueue () { 
	
		return this.buffer;
	}
	
	public long getTimestamp () {
		
		return this.timestamp;
	}
	
	public AtomicInteger getLatch () {
		
		return this.latch;
	}
}

package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public class Generator {
	
	GeneratedBuffer [] buffers;
	volatile int next;
	
	Executor executor;
	GeneratorWorker [] workers;
	
	long timestamp = 0;
	long count, limit;
	
	private final int bufferSize;
	private final int numberOfThreads;
	private final int adsPerCampaign;
	private final long[][] ads;	
	private List<List<Integer>> positionsList;
	
	public Generator (int bufferSize, int numberOfThreads, int adsPerCampaign, long[][] ads, int coreToBind) {
		this.bufferSize = bufferSize;
		this.numberOfThreads = numberOfThreads;
		this.adsPerCampaign = adsPerCampaign;
		this.ads = ads;
		
		buffers = new GeneratedBuffer [2];
		for (int i = 0; i < buffers.length; i++)
			buffers[i] = new GeneratedBuffer (bufferSize, false, numberOfThreads); /* TODO */ 
		next = 0;
		
		timestamp = 0;
		count = 0;
		limit = 10000; /* TODO */
		
		int inputTupleSize = 128; //inputSchema.getTupleSize()
		createPositionsList(inputTupleSize);		
		//executor = Executors.newCachedThreadPool();
		
		workers = new GeneratorWorker [numberOfThreads];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new GeneratorWorker (this, positionsList.get(i).get(0), positionsList.get(i).get(1), i + coreToBind);
			//workers[i].configure();
			//executor.execute(workers[i]);
			Thread thread = new Thread(workers[i]);
			thread.start();
		}
		
		fillNext ();
	}
	
	public GeneratedBuffer getBuffer (int id) {
		return buffers[id];
	}
	
	public int getPointer () {
		return next;
	}
	
	public long getTimestamp () {
		return timestamp;
	}
	
	public int getAdsPerCampaign () {
		return this.adsPerCampaign;
	};
	
	public long [][] getAds () {		
		return this.ads;
	};
	
	public GeneratedBuffer getNext () throws InterruptedException {
		GeneratedBuffer buffer = buffers[next];
		/* Is buffer `next` generated? */
		while (! buffer.isFilled())
			Thread.yield();
		fillNext ();
		/* Lock and return the current buffer */
		return buffer.lock();
	}
	
	public void fillNext () {
		
		int id;
		
		/* Set time stamp */
		if (count >= limit) {
			count = 0;
			timestamp ++;
		}
		/* Buffer swap */
		id = (next + 1) & (buffers.length - 1);
		
		// System.out.println("Fill buffer " + id);
		
		GeneratedBuffer buffer = buffers[id];
		/*
		 * The buffer can't be locked because a call to getNext() by a single consumer
		 * entails unlocking the previously used buffer.
		 */
		if (buffer.isLocked())
			throw new IllegalStateException ();
		
		/* Schedule N worker threads to fill this buffer. */
		buffer.setLatch (workers.length);
		
		/* Unblock all workers */
		next = (next + 1) & (buffers.length - 1);
	}

	public void createPositionsList (int inputTupleSize) {
    	int i;
    	int startPos = 0;
    	int incrementStep = (bufferSize % numberOfThreads == 0)? bufferSize / numberOfThreads : ((int) (bufferSize / numberOfThreads / inputTupleSize) * inputTupleSize);   	
    	int endPos = 0;
    	
    	ArrayList<Integer> threadList;
    	positionsList = new ArrayList<List<Integer>>(numberOfThreads);
    	for (i = 0; i < numberOfThreads; i++) {
    		threadList = new ArrayList<Integer>();
    		threadList.add(startPos);
    		endPos += incrementStep;
    		if (i == (numberOfThreads - 1) && endPos != bufferSize)
    			endPos = bufferSize;
    		threadList.add(endPos - 1);
    		startPos = endPos;
    		positionsList.add(threadList);
    	}
	}
	
}

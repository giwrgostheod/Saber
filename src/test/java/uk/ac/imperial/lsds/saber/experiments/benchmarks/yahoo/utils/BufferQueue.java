package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public class BufferQueue {

	private final int numberOfThreads;
	private final ITupleSchema inputSchema;
	private final int bufferSize;
	private final int timestampIterations;
	private List<List<Integer>> positionsList;
	private final SynchronizedFlag flag;
	
	private volatile BufferNode buffer;
	
	public BufferQueue (int numberOfThreads, ITupleSchema schemaToGenerate, int bufferSize, int timestampIterations, SynchronizedFlag flag) {
		this.numberOfThreads = numberOfThreads;
		this.inputSchema = schemaToGenerate;
    	this.bufferSize = bufferSize;
    	this.timestampIterations = timestampIterations;
    	this.flag = flag;
		createBufferQueue();
	}
	
	private void createBufferQueue () {
    	int i;
    	long start = System.nanoTime();    	
    	int startPos = 0;
    	int incrementStep = (bufferSize % numberOfThreads == 0)? bufferSize / numberOfThreads : ((int) (bufferSize / numberOfThreads / inputSchema.getTupleSize()) * inputSchema.getTupleSize()) ;    	
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
    	
	    buffer = new BufferNode(ByteBuffer.allocate(bufferSize), start, timestampIterations, numberOfThreads);
	    buffer.getReadLatch().setCount(timestampIterations);//buffer.isRead().set(timestampIterations);
	    //buffer.isReady().set(false);    	
	}
	
	public BufferNode getNext () {

		BufferNode tempBuffer;
/*		while (!buffer.isReady().get())
			Thread.yield();*/
		
		while (!flag.getFlag())
			Thread.yield();
		try {
			flag.reverseFlag();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

/*		while (buffer.getThreadsLatch().getCount() != 0)
			Thread.yield();*/
		
		tempBuffer = buffer;
    	//System.out.println("Got buffer.");

		return tempBuffer;
	}
	
	public void release (BufferNode buffer) {
		//System.out.println("bbbbb " + buffer.getReadLatch().getCount());
/*		try {
			while (buffer.getReadLatch().getCount() == 0)
				Thread.yield();
			buffer.getReadLatch().countDownOrWaitIfZero();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/

	}
	
	public List<List<Integer>> getPositionsList () {
		
		return this.positionsList;
	}
	
	public int getNumberOfThreads () {
		
		return this.numberOfThreads;
	}
	
	public ITupleSchema getInputSchema () {
		
		return this.inputSchema;
	}
	
	public BufferNode getBuffer () {
		return this.buffer;
	}
}

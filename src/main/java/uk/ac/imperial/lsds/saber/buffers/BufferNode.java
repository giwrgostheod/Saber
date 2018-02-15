package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferNode {

	private volatile ByteBuffer buffer;
	private long previousActualTimestamp;
	private final int timestampIterations;
	private AtomicInteger read;
	private AtomicBoolean ready;
	private volatile CountUpAndDownLatch threadsLatch, readLatch;

	public BufferNode(ByteBuffer buffer, long timestamp, int timestampIterations, int numberOfThreads) {
		this.buffer = buffer;
		this.previousActualTimestamp = timestamp;
		this.timestampIterations = timestampIterations;
		this.read = new AtomicInteger(timestampIterations);
		this.ready = new AtomicBoolean(false);
		
		this.threadsLatch = new CountUpAndDownLatch(numberOfThreads);
		this.readLatch = new CountUpAndDownLatch(timestampIterations);
	}

	public void setBuffer (ByteBuffer buffer) { 
		this.buffer = buffer; 
	}
	
	public void setPreviousTimestamp (long previousActualTimestamp) { 
		this.previousActualTimestamp = previousActualTimestamp; 
	}	
	
	public ByteBuffer getBuffer() { 
		return buffer; 
	}
	
	public long getPreviousTimestamp() { 
		return previousActualTimestamp; 
	}
	
	public AtomicInteger isRead() { 
		return read; 
	}
	
	public AtomicBoolean isReady() { 
		return ready; 
	}
	
	public int getTimestampIterations () {
		return timestampIterations;
	}
	
	public CountUpAndDownLatch getThreadsLatch() {
		return this.threadsLatch;
	}

	public CountUpAndDownLatch getReadLatch() {
		return this.readLatch;
	}
}
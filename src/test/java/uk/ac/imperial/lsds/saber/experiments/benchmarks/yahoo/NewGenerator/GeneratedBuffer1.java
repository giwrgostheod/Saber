package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.NewGenerator;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator.Latch;

public class GeneratedBuffer1 {
	
	ByteBuffer buffer;
	CountDownLatch latch;
	SynchronizedFlag1 bufferFilledLatch, bufferReadLatch;
	
	public GeneratedBuffer1 (int capacity, boolean direct, int numberOfThreads) {
		buffer = (direct) ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
		bufferFilledLatch = new SynchronizedFlag1(numberOfThreads);
		bufferReadLatch = new SynchronizedFlag1(1);
	}
	
	public ByteBuffer getBuffer() {
		return buffer;
	}
	
	public boolean isDirect () {
		return buffer.isDirect();
	}

	public boolean isFilled () {
		/* Latch is zero */
		while (bufferFilledLatch.getFlag() != 0)
			;
		return true;
	}

	public GeneratedBuffer1 lock () throws InterruptedException {
		bufferReadLatch.subtractFlag();
		return this;
	}
	
	public void unlock() throws InterruptedException {
		bufferReadLatch.incrementFlag();
	}

	public boolean isLocked() {
		return (bufferReadLatch.getFlag() == 0);
	}

	public void setLatch(int count) throws InterruptedException {
		/* Set latch to count */
		bufferFilledLatch.setCount(count);;
		// System.out.println("Latch is set to: " + bufferFilledLatch.getCount());
	}

	public void decrementLatch () throws InterruptedException {		
		bufferFilledLatch.subtractFlag();
		// System.out.println("Latch is decreased to: " + bufferFilledLatch.getCount());
	}
}
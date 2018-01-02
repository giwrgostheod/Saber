package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import uk.ac.imperial.lsds.saber.devices.TheCPU;

public class WorkerThread implements Runnable {

	private final List<Integer> positions;
	private final int adsPerCampaign;
	private final long [][] ads;
	private long timestamp;
	private EventGenerator eventGenerator;
	private AtomicInteger latch;
	
	int id;
	private ByteBuffer bufferHelper;
	private final int timestampCounter;
	
	private volatile BufferNode buffer;
	
	public WorkerThread(EventGenerator gen, List<Integer> positions, int id) {
		this.id = id;
		this.eventGenerator = gen;
		
		this.buffer = gen.getBufferQueue().getBuffer();
		this.timestampCounter = buffer.getTimestampIterations();
		this.adsPerCampaign = gen.getAdsPerCampaign();
		this.ads = gen.getAds();
		this.latch = gen.getLatch();
		
		this.timestamp = -1;				
		this.positions = positions;		
		bufferHelper = ByteBuffer.allocate(32);
	}

	public void run() {
		
		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, id));

		long currentTimestamp, _timestamp;
		int startPos = positions.get(0);
		int endPos = positions.get(1);
		int counter = 0;
		while (true) {
			
    		_timestamp = eventGenerator.getTimestamp();
    		
    		while (buffer.getThreadsLatch().getCount() == 0)
				Thread.yield();
	
    		if (counter == timestampCounter - 1) {
    			while (eventGenerator.getTimestamp() == timestamp)
    				Thread.yield();
    			_timestamp = eventGenerator.getTimestamp();
    			timestamp = _timestamp;
    			counter = 0;
    		}
       
			generate(buffer, startPos, endPos, _timestamp);
			
			try {
				while (buffer.getThreadsLatch().getCount() == 0)
					Thread.yield();
				buffer.getThreadsLatch().countDownOrWaitIfZero();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			//System.out.println("latch: " +buffer.getThreadsLatch().getCount());
			//buffer.isRead().set(false);

			currentTimestamp = System.nanoTime();
/*			if (buffers[i].getPreviousTimestamp() + 1000 > currentTimestamp) {
				System.err.println("The generation is slower than required");
				System.exit(1);
			}*/
			buffer.setPreviousTimestamp(currentTimestamp);
			counter ++;
		}
	}

	private void generate(BufferNode bufferNode, int startPos, int endPos, long timestamp) {
		
		ByteBuffer buffer = bufferNode.getBuffer().duplicate();
		/* Fill the buffer */	
		UUID user_id = UUID.randomUUID(); 
		UUID page_id = UUID.randomUUID();
		int value = 0;
		
		bufferHelper.clear();
		bufferHelper.putLong(user_id.getMostSignificantBits());                            // user_id
		bufferHelper.putLong(user_id.getLeastSignificantBits());
		bufferHelper.putLong(page_id.getMostSignificantBits());                            // page_id
		bufferHelper.putLong(page_id.getLeastSignificantBits());

		buffer.position(startPos);
		while (buffer.position()  < endPos) {

		    buffer.putLong (timestamp);		    
		    buffer.put(bufferHelper.array());
			buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][0]); // ad_id
			buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][1]);			
			buffer.putInt((value % 100000) % 5);                                         // ad_type: (0, 1, 2, 3, 4) => 
			                                                                             // ("banner", "modal", "sponsored-search", "mail", "mobile")
			buffer.putInt((value % 100000) % 3);                                         // event_type: (0, 1, 2) => 
																						 // ("view", "click", "purchase")
			
			buffer.putInt(1);                                                            // ip_address
			
			// buffer padding
			buffer.position(buffer.position() + 60);
			value ++;
		}	
	}
}

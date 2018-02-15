package uk.ac.imperial.lsds.saber.buffers;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.devices.TheCPU;

public class CircularBufferWorker implements Runnable {
	
	private CircularQueryBuffer circularBuffer;
	volatile boolean started = false;
	
	private int isFirstTime = 2;
	private ByteBuffer bufferHelper;
	private final int adsPerCampaign;
	private final long [][] ads;
	private final int id;
	public int value = 0;
	private ByteBuffer helper;
	
	public CircularBufferWorker (CircularQueryBuffer circularBuffer, int id) {
		this.circularBuffer = circularBuffer;
		this.adsPerCampaign = 10;//generator.getAdsPerCampaign();
		
		// Fix generator
		IPredicate joinPredicate = new LongLongComparisonPredicate
				(LongLongComparisonPredicate.EQUAL_OP , new LongLongColumnReference(1), new LongLongColumnReference(0));		
		CampaignGenerator campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate);
		this.ads = campaignGen.getAds();//generator.getAds();
		this.id = id;
		
		bufferHelper = ByteBuffer.allocate(32);
	}
	
	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {
		
	}
	
	@Override
	public void run() {
		
		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, id));

		int curr;
		int prev = -1;
		long timestamp;
		
		started = true;
		
		while (true) {
			
			/*while ( (curr = this.circularBuffer.counter) == prev)
				Thread.yield();*/
			
			while ( (curr = this.circularBuffer.isReady.get()) == prev)
				Thread.yield();						
			
			/* Fill buffer... */
			timestamp = this.circularBuffer.timestamp;
			int step = this.circularBuffer.globalLength; // power of two
			int startPos = this.circularBuffer.globalIndex + (id - 1)*step;
			int endPos = startPos + step;
			
			int startIndex = (id - 1)*step;
			
			int size = this.circularBuffer.size;
			
			if (started) {
				helper = ByteBuffer.allocate(32*32768);
				started = false;
			}
				
			
			ByteBuffer buffer = this.circularBuffer.getByteBuffer().duplicate();
			
			if (step > (size - startPos)) {
				int right = size - startPos;
				int left  = step - (size - startPos);
				
				System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, right);
				System.arraycopy(this.circularBuffer.inputBuffer, size - startPos, buffer.array(), 0, left);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), right, size, timestamp);
				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);

			} else {
				System.arraycopy(this.circularBuffer.inputBuffer, startIndex, buffer.array(), startPos, step);

				//writeToBuffer(this.circularBuffer.getByteBuffer(), startPos, endPos, timestamp);
			}
			
			prev = curr;
					
			this.circularBuffer.isBufferFilledLatch.decReaders();
			
			//this.circularBuffer.latch.countDown();
		}
	}	
	
	private void writeToBuffer(ByteBuffer inputBuffer, int startPos, int endPos, long timestamp) { 
		
		if (value > 100000)
			value=0;
		
		ByteBuffer buffer = inputBuffer.duplicate();
		/* Fill the buffer */		
		UUID user_id = UUID.randomUUID(); 
		UUID page_id = UUID.randomUUID();
		
		bufferHelper.clear();
		bufferHelper.putLong(user_id.getMostSignificantBits());                            // user_id
		bufferHelper.putLong(user_id.getLeastSignificantBits());
		bufferHelper.putLong(page_id.getMostSignificantBits());                            // page_id
		bufferHelper.putLong(page_id.getLeastSignificantBits());
		
		buffer.position(startPos);
		while (buffer.position()  < endPos) {

		    buffer.putLong (timestamp);		    
		    buffer.put(bufferHelper.array());
			buffer.putLong(this.ads[value % (100 * this.adsPerCampaign)][0]); // ad_id
			buffer.putLong(this.ads[value % (100 * this.adsPerCampaign)][1]);			
			buffer.putInt(value % 5);                                         // ad_type: (0, 1, 2, 3, 4) => 
			                                                                             // ("banner", "modal", "sponsored-search", "mail", "mobile")
			buffer.putInt(value % 3);                                         // event_type: (0, 1, 2) => 
																						 // ("view", "click", "purchase")
			
			buffer.putInt(1);                                                            // ip_address
			
			// buffer padding
			buffer.position(buffer.position() + 60);
			value ++;
		}
	}

}

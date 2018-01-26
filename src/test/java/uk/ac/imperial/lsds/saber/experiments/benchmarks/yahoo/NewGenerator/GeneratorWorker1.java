package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.NewGenerator;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.devices.TheCPU;

public class GeneratorWorker1 implements Runnable {
	
	Generator1 generator;
	volatile boolean started = false;
	
	private int isFirstTime = 2;
	private ByteBuffer bufferHelper;
	private final int adsPerCampaign;
	private final long [][] ads;
	private final int startPos;
	private final int endPos;
	private final int id;
	
	public GeneratorWorker1 (Generator1 generator, int startPos, int endPos, int id) {
		this.generator = generator;
		this.adsPerCampaign = generator.getAdsPerCampaign();
		this.ads = generator.getAds();
		this.startPos = startPos;
		this.endPos = endPos;
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
		GeneratedBuffer1 buffer;
		int prev = 0;
		long timestamp;
		
		started = true;
		
		while (true) {
			
			while ((curr = generator.next) == prev)
				;
			
			// System.out.println("Filling buffer " + curr);
			
			buffer = generator.getBuffer (curr);
			
			/* Fill buffer... */
			timestamp = generator.getTimestamp ();
			generate(buffer, startPos, endPos, timestamp);
			
			try {
				buffer.decrementLatch ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			prev = curr;
			// System.out.println("done filling buffer " + curr);
			// break;
		}
		// System.out.println("worker exits " );
	}
	
	private void generate(GeneratedBuffer1 generatedBuffer, int startPos, int endPos, long timestamp) {
		
		
		ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		/* Fill the buffer */	
		
		if (isFirstTime!=0 ) {
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
			isFirstTime --;
		} else {
			buffer.position(startPos);
			while (buffer.position()  < endPos) {
	
			    buffer.putLong (timestamp);
				
				// buffer padding
				buffer.position(buffer.position() + 120);
			}
		}	
	}

}

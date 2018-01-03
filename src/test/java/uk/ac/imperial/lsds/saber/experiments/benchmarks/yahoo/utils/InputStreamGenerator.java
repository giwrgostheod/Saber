package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public class InputStreamGenerator {
	
	private ITupleSchema inputSchema;
	private byte [] data;
	private ByteBuffer buffer;
	private int adsPerCampaign;
	
	private int tuplesPerInsert; 	// recordsPerSecond =  5000000	// recordsPerSecond = 80000000
	private long [][] ads;
	private long timestampReference = -1L;	
	private long lastGeneratedTimestamp = 0L;	
	private long startTimestamp = 0L;
	private boolean createFile;
	
	
	public InputStreamGenerator (ITupleSchema inputSchema, int adsPerCampaign, int tuplesPerInsert, long [][] ads, boolean createFile) {
		this.inputSchema = inputSchema;
		this.tuplesPerInsert = tuplesPerInsert;
		this.createFile = createFile;
		
		if (!createFile) {
			this.data = new byte [inputSchema.getTupleSize() * this.tuplesPerInsert];
			this.buffer = ByteBuffer.wrap(data);
		}
		this.adsPerCampaign = adsPerCampaign;
		this.ads = ads;
		this.timestampReference = System.nanoTime();
		this.startTimestamp = 0L;
	}

	public ByteBuffer generateNext (boolean realtime) throws Exception {	
		
		if (createFile)
			throw new Exception("There is no buffer instatiated, as the generator was created to generate a file.");
		
		//buffer.clear();
		/* Fill the buffer */	
		UUID user_id = UUID.randomUUID(); 
		UUID page_id = UUID.randomUUID();
		int value = 0;
		this.timestampReference = System.nanoTime();
		
		long timestamp = 0;//timestampReference;
		
		while (buffer.hasRemaining()) {
			
			if (realtime) {
				if (startTimestamp == 0) {
					startTimestamp = System.currentTimeMillis();
					buffer.putLong (0);
				} else {
					buffer.putLong (System.currentTimeMillis() - startTimestamp);
				}
			}
			else {
				buffer.putLong (timestamp);
			}
			
			buffer.putLong(user_id.getMostSignificantBits());                            // user_id
			buffer.putLong(user_id.getLeastSignificantBits());
			
			buffer.putLong(page_id.getMostSignificantBits());                            // page_id
			buffer.putLong(page_id.getLeastSignificantBits());
			
			buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][0]); // ad_id
			buffer.putLong(this.ads[(value % 100000) % (100 * this.adsPerCampaign)][1]);
			
			buffer.putInt((value % 100000) % 5);                                         // ad_type: (0, 1, 2, 3, 4) => 
			                                                                             // ("banner", "modal", "sponsored-search", "mail", "mobile")
			buffer.putInt((value % 100000) % 3);                                         // event_type: (0, 1, 2) => 
			                                                                             // ("view", "click", "purchase")
			buffer.putInt(1);                                                            // ip_address
			
			/* Additional fields to simulate the strings*/
			/* buffer.putLong(0L); 
			buffer.putInt(0);
			buffer.putLong(0L);	*/
			
			// buffer padding
			buffer.put(this.inputSchema.getPad());
						
			timestamp ++; //System.nanoTime();
			value ++;
		}
		lastGeneratedTimestamp = timestamp;
		return buffer;
	}
	
	public ByteBuffer updateTimestamps (boolean realtime) throws Exception {
		
		if (createFile)
			throw new Exception("There is no buffer instatiated, as the generator was created to generate a file.");
		
		int i = 0;
		if (buffer.position() != buffer.capacity()) {
			System.err.println("Unlikely error");
			System.exit(1);
		}
		long timestamp = lastGeneratedTimestamp;
		for (i = 0; i < buffer.position(); i+= inputSchema.getTupleSize()) {
			if (realtime)
				buffer.putLong (i, System.currentTimeMillis() - startTimestamp);
			else
				buffer.putLong(i, ++timestamp);
		}
		lastGeneratedTimestamp = timestamp;
		return buffer;
	}
	
	public void generateDummyInputFile (boolean realtime) {
		String mode = "datafile5000000RecordsPerSecond.dat";
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(mode, false));
			
			UUID user_id = UUID.randomUUID(); 
			UUID page_id = UUID.randomUUID();
			
			long 	timestamp;
			long 	userIdMSB;
			long 	userIdLSB;   
			long 	pageIdMSB;
			long 	pageIdLSB;
			long 	  adIdMSB;
			long 	  adIdLSB;
			int 	   adType;  
			int  	eventType; 
			int  	ipAddress;
			//int 	  queryId;

			int value = 0;
			this.timestampReference = System.nanoTime();
			
			timestamp = 0;//timestampReference;
			for (int i = 0; i < tuplesPerInsert; i++ ) {

				if (realtime) {
					if (startTimestamp == 0) {
						startTimestamp = System.currentTimeMillis();
						timestamp = 0L;
					} else {
						timestamp = (System.currentTimeMillis() - startTimestamp);
					}
				}
				else {
					timestamp++;
				}
				
				userIdMSB = user_id.getMostSignificantBits();
				userIdLSB = user_id.getLeastSignificantBits();
				pageIdMSB = page_id.getMostSignificantBits();
				pageIdLSB = page_id.getLeastSignificantBits();
				adIdMSB = this.ads[(value % 100000) % (100 * this.adsPerCampaign)][0];
				adIdLSB = this.ads[(value % 100000) % (100 * this.adsPerCampaign)][1];			
				adType = (value % 100000) % 5;
				eventType = (value % 100000) % 3; 
				ipAddress = 1;				
							
				value ++;
	    		//queryId = 0;
		    				
	    		pw.print(timestamp + "," + userIdMSB + "," + userIdLSB + "," + pageIdMSB + "," + pageIdLSB);
	    		pw.print("," + adIdMSB + "," + adIdLSB + "," + adType + "," + eventType + "," + ipAddress);
	    		pw.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
		    // Make sure to close the file when done
			pw.flush();
		    pw.close();
		}
	}
	
	public long getTimestampReference () {
		return this.timestampReference;
	}
}

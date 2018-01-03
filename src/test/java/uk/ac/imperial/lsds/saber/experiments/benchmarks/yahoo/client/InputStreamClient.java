package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.client;

import java.net.InetSocketAddress;
import java.net.InetAddress;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.InputStreamGenerator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class InputStreamClient {
	
	@SuppressWarnings("unused")
	private static final String usage = "usage: java InputStreamClient";
	
	private static YahooBenchmark benchmarkQuery;
	
	public static void createBenchmarkQuery () {
		int batchSize = 1048576;
		int numberOfThreads = 1;
		String executionMode = "cpu";
		int circularBufferSize = 64 * 1048576;
		int unboundedBufferSize = 1 * 1048576 ;
		int hashTableSize = 8 * 65536; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 4; 	
		QueryConf queryConf = new QueryConf (batchSize);		
		SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;		
		SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;		
		SystemConf.HASH_TABLE_SIZE = hashTableSize;		
		SystemConf.PARTIAL_WINDOWS = partialWindows;		
		SystemConf.SWITCH_THRESHOLD = 10;		
		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;		
		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		SystemConf.THREADS = numberOfThreads;
		SystemConf.LATENCY_ON = false;		
		
		// Initialize the Operators of the Benchmark
		benchmarkQuery = new YahooBenchmark (queryConf, false);
	}
	
 	public static ITupleSchema createInputStreamSchema () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* Event Time Timestamp:	long */
		offsets[1] =  8; /* User Id:   				uuid */		
		offsets[2] = 24; /* Page Id: 				uuid */
		offsets[3] = 40; /* Ad Id:   				uuid */  
		offsets[4] = 56; /* Ad Type:                 int */  // (0, 1, 2, 3, 4): ("banner", "modal", "sponsored-search", "mail", "mobile") 
		                                                     // => 16 bytes required if UTF-8 encoding is used
		offsets[5] = 60; /* Event Type:              int */  // (0, 1, 2)      : ("view", "click", "purchase")
		                                                     // => 8 bytes required if UTF-8 encoding is used
		offsets[6] = 64; /* IP Address:              int */  // 255.255.255.255 => Either 4 bytes (IPv4) or 16 bytes (IPv6)
				
		ITupleSchema schema = new TupleSchema (offsets, 68);
		
		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
		schema.setAttributeType (0, PrimitiveType.LONG 	   );
		schema.setAttributeType (1, PrimitiveType.LONGLONG );
		schema.setAttributeType (2, PrimitiveType.LONGLONG );
		schema.setAttributeType (3, PrimitiveType.LONGLONG );
		schema.setAttributeType (4, PrimitiveType.INT  	   );
		schema.setAttributeType (5, PrimitiveType.INT      );
		schema.setAttributeType (6, PrimitiveType.INT      );
		
		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "user_id");
		schema.setAttributeName (2, "page_id");
		schema.setAttributeName (3, "ad_id");
		schema.setAttributeName (4, "ad_type");
		schema.setAttributeName (5, "event_type");
		schema.setAttributeName (6, "ip_address");	
		
		//schema.setName("InputStream");
		return schema;
	}

	public static void generateData() {	
		/* Create Input Schema */
		ITupleSchema inputSchema = createInputStreamSchema();
		/* Generate input stream */
		//ITupleSchema schemaToGenerate = benchmarkQuery.getSchema();
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();
		
		int records = 5000000; 	// recordsPerSecond =  5000000 or 80000000
		
		boolean realtime = true;
		System.out.println("Generating data...");
		InputStreamGenerator gen = new InputStreamGenerator(inputSchema, adsPerCampaign, records, ads, true);
		gen.generateDummyInputFile(realtime);
		System.out.println("Data is generated!");
		//long timestampReference = gen.getTimestampReference();		
	}
	
	public static void main (String[] args) {
		
		// Bind the Client Side in the last core
		int coreToBind = 5;
		TheCPU.getInstance().bind(coreToBind);
		
		// Initialize the variables required for generating data
		createBenchmarkQuery();
		
		String hostname = "localhost";
		int port = 6667;
		
		int reports = 50000000; // 80000000;
		int tupleSize = 128;
		
		int _BUFFER_ = tupleSize * reports;
		ByteBuffer data = ByteBuffer.allocate(_BUFFER_);
		
		int bundle = 512;
		
		int L = 1;		
		
		// Generate Data
		boolean generate = false;//true;
		if (generate) {
			generateData();
		}
		
		String filename = "datafile5000000RecordsPerSecond.dat";
		
		FileInputStream f;
		DataInputStream d;
		BufferedReader  b;
		
		String line = null;
		long lines = 0;
		long MAX_LINES = 12048577L;
		long percent_ = 0L, _percent = 0L;
		
		/* Time measurements */
		long start = 0L;
		long bytes = 0L;
		double dt;
		double rate; /* tuples/sec */
		double _1MB = 1024. * 1024.;
		double MBps; /* MB/sec */
		long totalTuples = 0;
		
		long wrongTuples = 0L;		
		
		InputStreamTuple tuple = new InputStreamTuple ();
		
		try {
			/* Establish connection to the server */
			SocketChannel channel = SocketChannel.open();
			channel.configureBlocking(true);
			InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(hostname), port);
			/* System.out.println(address); */
			channel.connect(address);
		
			while (! channel.finishConnect())
				;
			
			/* Load file into memory */
			String path = "/home/george/saber/yahoo_benchmark_saber/";
			f = new FileInputStream(path + filename);
			d = new DataInputStream(f);
			b = new BufferedReader(new InputStreamReader(d));
			
			start = System.currentTimeMillis();
			
			while ((line = b.readLine()) != null) {
				lines += 1;
				bytes += line.length() + 1; // +1 for '\n'
				
				percent_ = (lines * 100) / MAX_LINES;
				if (percent_ == (_percent + 1)) {
					System.out.print(String.format("Loading file...%3d%%\r", percent_));
					_percent = percent_;
				}
				
				InputStreamTuple.parse(line, tuple);
				
				/* if (tuple.getPosition() < 0) {
					wrongTuples += 1;
					continue;
				}

				if (tuple.getType() != 0) {
					continue;
				}*/
				
				totalTuples += 1;
				
				/* Populate data */
				data.putLong  (tuple.getTimestamp()       );
				data.putLong  (tuple.getUserIdMSB()       );
				data.putLong  (tuple.getUserIdLSB()       );
				data.putLong  (tuple.getPageIdMSB()       );
				data.putLong  (tuple.getPageIdLSB()       );
				data.putLong  (tuple.getAdIdMSB()         );
				data.putLong  (tuple.getAdIdLSB()         ); 
				data.putInt   (tuple.getAdType()          );
				data.putInt   (tuple.getEventType()       );
				data.putInt   (tuple.getIpAddress()       );
				
				// Add Padding to the buffer
				data.putLong(0L);
				data.putLong(0L);
				data.putLong(0L);
				data.putLong(0L);
				data.putLong(0L);
				data.putLong(0L);
				data.putLong(0L);
				data.putInt  (0);
			
				
				d.close();
				dt = (double ) (System.currentTimeMillis() - start) / 1000.;
				/* Statistics */
				rate =  (double) (lines) / dt;
				MBps = ((double) bytes / _1MB) / dt;
				
				System.out.println(String.format("[DBG] %10d lines read", lines));
				System.out.println(String.format("[DBG] %10d bytes read", bytes));
				System.out.println(String.format("[DBG] %10d tuples in data buffer", totalTuples));
				System.out.println();
				System.out.println(String.format("[DBG] %10.1f seconds", (double) dt));
				System.out.println(String.format("[DBG] %10.1f tuples/s", rate));
				System.out.println(String.format("[DBG] %10.1f MB/s", MBps));
				System.out.println(String.format("[DBG] %10d tuples ignored", wrongTuples));
				System.out.println();
			} 
			
			
			/* Prepare data for reading */
			data.flip();
			
			/* Buffer to sent */
			ByteBuffer buffer = ByteBuffer.allocate(tupleSize * bundle);
			System.out.println(String.format("[DBG] %6d bytes/buffer", _BUFFER_));
			/* Tuple buffer */
			byte [] t = new byte [tupleSize];
			
			totalTuples = 0L;
			bytes = 0L;
			
			long _bundles = 0L;
			while (data.hasRemaining()) {
				data.get(t);
				totalTuples += 1;
				for (int i = 0; i < L; i++) {
					buffer.put(t);
					//ByteBuffer.wrap(t).putInt(20, i + 1); /* Position 20 is the highway id */
					
					if (! buffer.hasRemaining()) {
						/* Bundle assembled. Send and rewind. */
						_bundles ++;
						buffer.flip();
						/* 
 						 * System.out.println(String.format("[DBG] send bundle %d (%d bytes remaining)", 
						 * _bundles, buffer.remaining()));
						 */
						bytes += channel.write(buffer);
						/* Rewind buffer and continue populating with tuples */
						buffer.clear();
					}
				}
			}
		
			/* Sent last (incomplete) bundle */
			buffer.flip();
			bytes += channel.write(buffer);
		
			System.out.println(String.format("[DBG] %10d tuples processed %d bundles (%d bytes) sent", 
					totalTuples, _bundles, bytes));
			
			System.out.println("Bye.");
			
		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
}


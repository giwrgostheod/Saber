package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;


import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;

public class YahooBenchmarkAppWithClientInMemoryGeneration {
	
	public static final String usage = "usage: YahooBenchmarkApp";
	
	public static void main (String [] args) {

		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		String hostname = "localhost";//"192.168.10.98";
		int port = 6667;
		int batchSize = 1048576;		
		
		int numberOfThreads = 3;
		String executionMode = "cpu";
		int circularBufferSize = 64 * 1048576;
		int unboundedBufferSize = 1 * 1048576 ;
		int hashTableSize = 8 * 65536; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 4; // 64; // 1048576;
		
		
		
		// Set SABER's configuration				
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
		
		int networkBufferSize = 2 * 1048576;//bundle * benchmarkQuery.getSchema().getTupleSize();
		System.out.println(String.format("[DBG] Initializing the server side..."));		
		System.out.println(String.format("[DBG] The size of the buffer used for incoming data is %6d", networkBufferSize));		

		
		System.out.println(String.format("[DBG] Waiting for incoming connections..."));		
		// Begin Execution
		try {	
			ServerSocketChannel server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress (hostname, port));
			
			System.out.println("[DBG] ^");
			ByteBuffer buffer = ByteBuffer.allocate (networkBufferSize);
			ByteBuffer campaigns = ByteBuffer.allocate (32 * 1000);
			
			server.configureBlocking(false);
			
			@SuppressWarnings("unused")
			int bytes;
			boolean receivedCampaigns = false;
			while(!receivedCampaigns){
			    SocketChannel socketChannel = server.accept();

			    /* Pass the campaigns before the other data.*/
			    if (socketChannel != null){
			    	
			    	while(!receivedCampaigns) {			    					
						bytes = 0;
						if((bytes = socketChannel.read(campaigns)) > 0) {
							if (!campaigns.hasRemaining()) {
								campaigns.flip();
				    			// Initialize the Operators of the Benchmark
				    			benchmarkQuery = new YahooBenchmark (queryConf, true, campaigns);
								System.out.println(String.format("[DBG] Read the campaigns along with their ads."));		
				    			receivedCampaigns = true;
							}		
						}
			    	}
			    }
			}
			
			System.out.println(String.format("[DBG] Ready to receive input..."));		
			while(true){
			    SocketChannel socketChannel = server.accept();

			    if (socketChannel != null){			    	
			    	
					while (true) {
						bytes = 0;
						if((bytes = socketChannel.read(buffer)) > 0) {
							//System.out.println(String.format("[DBG] %6d bytes received", bytes));
							if (!buffer.hasRemaining()) {
								buffer.rewind();
								benchmarkQuery.getApplication().processData (buffer.array(), buffer.capacity());
								buffer.clear();
							}
						}
							
					}
			    }
			}

		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
	//================================================================================
}

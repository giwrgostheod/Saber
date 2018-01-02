package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;

public class YahooBenchmarkAppWithClient {
	
	public static final String usage = "usage: YahooBenchmarkApp";
	
	public static void main (String [] args) {

        //================================================================================
		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		String hostname = "localhost";
		int port = 6667;
		int bundle = 512;
		int batchSize = 1048576;
		int numberOfThreads = 7;
		String executionMode = "cpu";
		int circularBufferSize = 64 * 1048576;
		int unboundedBufferSize = 1 * 1048576 ;
		int hashTableSize = 8 * 65536; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 4; // 64; // 1048576;
		
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("--mode")) { 
				executionMode = args[j];
			} else
			if (args[i].equals("--threads")) {
				numberOfThreads = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--batch-size")) { 
				batchSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--circ-buffer-size")) { 
				circularBufferSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--unbounded-buffer-size")) { 
				unboundedBufferSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--hash-table-size")) { 
				hashTableSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--partial-windows")) { 
				partialWindows = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--scheduling-policy")) { 
				SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.FIFO;
			} else
			if (args[i].equals("--host")) { 
				hostname = args[j];
			} else
			if (args[i].equals("--port")) { 
				port = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--bundle-size")) { 
				bundle = Integer.parseInt(args[j]);
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		//================================================================================
		
		
		
		//================================================================================
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
		//================================================================================
		
		
		
		//================================================================================
		// Initialize the Operators of the Benchmark
		benchmarkQuery = new YahooBenchmark (queryConf);
		int networkBufferSize = bundle * benchmarkQuery.getSchema().getTupleSize();
		System.out.println(String.format("[DBG] %6d bytes/buffer", networkBufferSize));
		//================================================================================
		
		
		
		//================================================================================
		// Begin Execution
		try {	
			ServerSocketChannel server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress (hostname, port));
			server.configureBlocking(false);
			
			Selector selector = Selector.open();
			/* (SelectionKey) */ server.register(selector, SelectionKey.OP_ACCEPT);
			
			System.out.println("[DBG] ^");
			ByteBuffer buffer = ByteBuffer.allocate (networkBufferSize);
			while (true) {
			
				if (selector.select() == 0)
					continue;
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iterator = keys.iterator();
				while (iterator.hasNext()) {
					
					SelectionKey key = iterator.next();
					
					if (key.isAcceptable()) {
						
						System.out.println("[DBG] key is acceptable");
						ServerSocketChannel _server = (ServerSocketChannel) key.channel();
						SocketChannel client = _server.accept();
						if (client != null) {
							System.out.println("[DBG] accepted client");
							client.configureBlocking(false);
							/* (SelectionKey) */ client.register(selector, SelectionKey.OP_READ);
						}
					} else if (key.isReadable()) {
						
						SocketChannel client = (SocketChannel) key.channel();
						int bytes = 0;
						if ((bytes = client.read(buffer)) > 0) {
							
							if (! buffer.hasRemaining()) {
								buffer.rewind();
								benchmarkQuery.getApplication().processData (buffer.array(), buffer.capacity());
								buffer.clear();
							}
						}
						if (bytes < 0) {
							System.out.println("[DBG] client connection closed");
							client.close();
						}
					} else {
						System.err.println("error: unknown selection key");
						System.exit(1);
					}
					iterator.remove();
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

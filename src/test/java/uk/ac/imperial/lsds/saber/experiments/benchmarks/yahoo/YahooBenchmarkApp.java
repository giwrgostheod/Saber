package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.BufferNode;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.BufferQueue;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.EventGenerator;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.SynchronizedFlag;

public class YahooBenchmarkApp {
	
	public static final String usage = "usage: YahooBenchmarkApp with small buffer generator";
	public static volatile BufferQueue bQueue;
	
	public static void main (String [] args) {

        //================================================================================
		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 7;
		int batchSize = 1048576;
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
		SystemConf.PERFORMANCE_MONITOR_INTERVAL = 1000L;
		//================================================================================
		
		
		
		//================================================================================
		// Initialize the Operators of the Benchmark
		benchmarkQuery = new YahooBenchmark (queryConf, true);
		//================================================================================
		
		
		
		//================================================================================
		/* Generate input stream */
		int numberOfGeneratorThreads = 3;
		//int tuplesPerSec = 100000000;
		//int tuplesPerInsert = 100000; 
		ITupleSchema schemaToGenerate = benchmarkQuery.getSchema();
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();
		
		//TheCPU.getInstance().bind(0);
		
		int bufferSize = 2 * 1048576;
		int timestampIterations = 4; //8;//64;
		int coreToBind = numberOfThreads + 1;
		SynchronizedFlag flag = new SynchronizedFlag(false);
		bQueue = new BufferQueue(numberOfGeneratorThreads, schemaToGenerate, bufferSize, timestampIterations, flag);
		EventGenerator eventGenerator = new EventGenerator(bQueue, adsPerCampaign, ads, coreToBind, flag);
    	Thread generator = new Thread(eventGenerator);
    	generator.start();
		//InputStreamGenerator gen = new InputStreamGenerator(inputSchema, adsPerCampaign, tuplesPerInsert, ads);
		//ByteBuffer buffer = gen.generateNext(realtime);
		//long timestampReference = 0; //gen.getTimestampReference();
		
		//================================================================================
		
		
		
		//================================================================================
		// Begin Execution
		
		try {
			BufferNode bufferNode = null;
			long time2, time1 = System.nanoTime();
			double throughput, dt;
			long sum = 0;
			while (true) {							

				bufferNode = bQueue.getNext();
				benchmarkQuery.getApplication().processData (bufferNode.getBuffer().array());
				
/*				sum += bufferNode.getBuffer().capacity() / 128;
				System.out.println(String.format("[DBG] %20d tuples/s", sum));
				if (sum >= tuplesPerSec) {
					time2 = System.nanoTime();
					dt = (double) (time2 - time1) / 1000000000D;
					throughput = ((double) sum) / dt;
					System.out.println(String.format("[DBG] %20.3f tuples/s", throughput));
					time1 = time2;
					sum = 0;
				}*/
				/*
				if (SystemConf.LATENCY_ON)
					bufferNode.getBuffer().putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), bufferNode.getBuffer().getLong(0)));
				*/
				bQueue.release(bufferNode);
				
			}
		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
	//================================================================================
}

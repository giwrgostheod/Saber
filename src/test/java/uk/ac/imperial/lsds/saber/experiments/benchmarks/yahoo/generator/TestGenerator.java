package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkQuery;

public class TestGenerator {
	public static final String usage = "usage: YahooBenchmarkApp with simpler in-memory generation";
	
	public static void main (String [] args) throws InterruptedException {

		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 1;
		int batchSize = 4 * 1048576; //8 * 1048576 / 2;
		String executionMode = "cpu";
		int circularBufferSize = 128 * 1 * 1048576/2; //32 * 4 * 1048576;
		int unboundedBufferSize = 4 * 1048576; //8 * 1048576 / 2;
		int hashTableSize = 2*64*128;//1 * 65536;//4 * 65536; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 2; //128; // 64; // 1048576;
		int slots = 1 * 128 * 1024;//128 * 1024;
		
		boolean isV2 = false;
		
		if (args.length!=0)  
			numberOfThreads = Integer.parseInt(args[0]);
		
		// Set SABER's configuration				
		QueryConf queryConf = new QueryConf (batchSize);		
		SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;		
		SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;		
		SystemConf.HASH_TABLE_SIZE = hashTableSize;		
		SystemConf.PARTIAL_WINDOWS = partialWindows;
		SystemConf.SLOTS = slots;
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
		benchmarkQuery = new YahooBenchmark (queryConf, true, null, isV2);
			
		
		/* Generate input stream */
		int numberOfGeneratorThreads = 2;
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();
		
		// TheCPU.getInstance().bind(0);
		
		int bufferSize = 4 * 131072;//4*32768;//1048576/32;
		int coreToBind = 3;//;numberOfThreads + 1;
		
		
		Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind, isV2);

		
/*		long time2, time1 = System.nanoTime();
		double throughput, dt;
		long sum = 0;*/
		
		//GeneratedBuffer b = generator.getNext();
/*		ByteBuffer helper = ByteBuffer.allocate(8*131072);
		
		while (helper.hasRemaining()) {
			helper.putInt(1);
		}*/

		long timeLimit = System.currentTimeMillis() + 10 * 10000;
		//GeneratedBuffer b = generator.getNext();

		while (true) {
			
			if (timeLimit <= System.currentTimeMillis()) {
				System.out.println("Terminating execution...");
				System.exit(0);
			}						
			

			GeneratedBuffer b = generator.getNext();

			//for (int k = 0; k < 9999; k++)
			benchmarkQuery.getApplication().processData (b.getBuffer().array());
				
			b.unlock();
				
			//System.arraycopy(b.getBuffer().array(), 0, helper.array(), 0, b.getBuffer().array().length);

/*			sum += b.getBuffer().capacity() / 128;
			//System.out.println(String.format("[DBG] %20d tuples/s", sum));
			if (sum >= 10000000) {
				time2 = System.nanoTime();
				dt = (double) (time2 - time1) / 1000000000D;
				throughput = ((double) sum) / dt;
				System.out.println(String.format("[DBG] %20.3f tuples/s", throughput));
				time1 = time2;
				sum = 0;
			}*/			
		}
	}
}

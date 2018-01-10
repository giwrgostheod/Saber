package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkQuery;

public class TestGenerator {
	public static final String usage = "usage: YahooBenchmarkApp with simpler in-memory generation";
	
	public static void main (String [] args) throws InterruptedException {

		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 5;
		int batchSize = 1048576;
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

		
		
		// Initialize the Operators of the Benchmark
		benchmarkQuery = new YahooBenchmark (queryConf, true);
			
		
		/* Generate input stream */
		int numberOfGeneratorThreads = 1;
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();
		
		TheCPU.getInstance().bind(0);
		
		int bufferSize = 5 * 1048576;
		int coreToBind = numberOfThreads + 1;
		
		
		Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind);

		
/*		long time2, time1 = System.nanoTime();
		double throughput, dt;
		long sum = 0;*/
		
		GeneratedBuffer b = generator.getNext();
		while (true) {
			
			//GeneratedBuffer b = generator.getNext();
			benchmarkQuery.getApplication().processData (b.getBuffer().array());

/*			sum += b.getBuffer().capacity() / 128;
			//System.out.println(String.format("[DBG] %20d tuples/s", sum));
			if (sum >= tuplesPerSec) {
				time2 = System.nanoTime();
				dt = (double) (time2 - time1) / 1000000000D;
				throughput = ((double) sum) / dt;
				System.out.println(String.format("[DBG] %20.3f tuples/s", throughput));
				time1 = time2;
				sum = 0;
			}*/
			
			//b.unlock();
			
		}
	}
}

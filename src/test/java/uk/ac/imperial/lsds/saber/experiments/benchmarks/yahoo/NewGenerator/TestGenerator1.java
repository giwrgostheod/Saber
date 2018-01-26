package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.NewGenerator;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkQuery;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator.GeneratedBuffer;

public class TestGenerator1 {
	public static final String usage = "usage: YahooBenchmarkApp with simpler in-memory generation";
	
	public static void main (String [] args) throws InterruptedException {

		/* Parse command line arguments */
		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 6;
		int batchSize = 1048576;
		String executionMode = "cpu";
		int circularBufferSize = 64 * 1048576 / 2;
		int unboundedBufferSize = 1 * 1048576 / 2;
		int hashTableSize = 4 * 65536 / 1; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 2; // 64; // 1048576;
		
		
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
		
		// TheCPU.getInstance().bind(0);
		
		int bufferSize = 2 * 1048576;
		int coreToBind = 1;//numberOfThreads + 1;
		
		
		Generator1 generator = new Generator1 (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind);

		
		long time2, time1 = System.nanoTime();
		double throughput, dt;
		long sum = 0;
		
		//GeneratedBuffer b = generator.getNext();
		ByteBuffer helper = ByteBuffer.allocate(4*1048576);
		
		long timeLimit = System.currentTimeMillis() + 10 * 10000;
		while (true) {
			
			if (timeLimit <= System.currentTimeMillis()) {
				System.out.println("Terminating execution...");
				System.exit(0);
			}			
			GeneratedBuffer1 b = generator.getNext();
			
			benchmarkQuery.getApplication().processData (b.getBuffer().array());
			b.unlock();

			for (int k = 0; k < 100000000; k++)
				benchmarkQuery.getApplication().processData (b.getBuffer().array());

			/*System.arraycopy(b.getBuffer().array(), 0, helper.array(), 0, b.getBuffer().array().length);

			sum += b.getBuffer().capacity() / 128;
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

package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;

public class Tester {

	static ByteBuffer b;
	static YahooBenchmark benchmarkQuery;
	
	public static void main(String[] args) {
		
		QueryConf queryConf = new QueryConf (1048576);		
		benchmarkQuery = new YahooBenchmark (queryConf);
		
		boolean isDirect = false;
		int bufferSize = 8 * 1048576;
		int passes = 1000;
		
		b = isDirect? ByteBuffer.allocateDirect(bufferSize): ByteBuffer.allocate(bufferSize)  ;
		
		ByteBuffer tuple = ByteBuffer.allocate(128);
		
		long time = System.nanoTime();
		
		for (int i = 0; i < passes; i++) {
			
			while (b.hasRemaining()) {
				// putTuble();
				putTuple (tuple);
			}
			b.clear();
		}
		double dt = ((double) (System.nanoTime() - time)) / 1000000000D;
		long bytes = (long) (passes) * (long) (bufferSize);
		double throughput = ((double) bytes) / dt;
		double tuplesPerSec = throughput / (double) benchmarkQuery.getSchema().getTupleSize();
		
		System.out.println("The buffer was Direct: " + isDirect);
		System.out.println("The time elapsed was : " + dt + " seconds and the bytes produced were " + bytes);
		// System.out.println("The throughput was : " + throughput + " and the tuples per second were " + tuplesPerSec);
		System.out.println(String.format("Throughput %20.3f bytes/s or %20.3f tuples/s", throughput, tuplesPerSec));

	}
	
	private static void putTuple (ByteBuffer tuple) {
		tuple.clear();
		tuple.putLong (1L);
		tuple.putLong (1L);                            // user_id
//		tuple.putLong (1L);
//		tuple.putLong (1L);                            // page_id
//		tuple.putLong (1L);
		tuple.putLong (1L); // ad_id
		tuple.putLong (1L);
		tuple.clear();
		b.put(tuple);
	}

	private static void putTuble() {
		
	    b.putLong (1L);
		b.putLong (1L);                            // user_id
		b.putLong (1L);
		b.putLong (1L);                            // page_id
		b.putLong (1L);
		b.putLong (1L); // ad_id
		b.putLong (1L);
		// b.putInt  (1);                                         // ad_type: (0, 1, 2, 3, 4) =>                                                                      // ("banner", "modal", "sponsored-search", "mail", "mobile")
		// b.putInt (1);                                         // event_type: (0, 1, 2) => 
		                                           // ("view", "click", "purchase")
		// b.putInt(1);                               // ip_address
		
		// buffer padding
		// b.put(benchmarkQuery.getSchema().getPad());
		b.position(b.position() + 12);
		b.position(b.position() + 60);
		
//		/* 128 - 68 = 60 */
//		for (int i = 0; i < 7; i++)
//			b.putLong (0L);
//		b.putInt(0); 
	}

}

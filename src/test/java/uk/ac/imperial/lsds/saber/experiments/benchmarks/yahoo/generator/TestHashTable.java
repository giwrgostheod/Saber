package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableWrapper;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmark;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkQuery;

public class TestHashTable {
	
	public static void main (String [] args) throws InterruptedException {
		
		SystemConf.HASH_TABLE_SIZE =  4 * 65536 / 2;		
		
		WindowHashTableWrapper hashT = new WindowHashTableWrapper();
		//hashT.configure(content, start, end, keyLength, valueLength);
		
		
	}
}

package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.client;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.generator.Generator;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;

public class InputStreamClientWithDataGeneration {
	
	@SuppressWarnings("unused")
	private static final String usage = "usage: java InputStreamClient";
		
	public static void main (String[] args) {
		
		// Bind the Client Side in the last core
		int coreToBind = 5;
		TheCPU.getInstance().bind(coreToBind);

		String hostname = "localhost";
		int port = 6667;
				
		try {
			/* Establish connection to the server */
			SocketChannel channel = SocketChannel.open();
			channel.configureBlocking(true);
			InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(hostname), port);
			/* System.out.println(address); */
			channel.connect(address);
		
			while (! channel.finishConnect())
				;
			
			
			/* Generate Campaigns*/
			IPredicate joinPredicate = new LongLongComparisonPredicate
					(LongLongComparisonPredicate.EQUAL_OP , new LongLongColumnReference(1), new LongLongColumnReference(0));
			int adsPerCampaign = 10;
			CampaignGenerator campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate);	
			long[][] ads = campaignGen.getAds();
			ByteBuffer campaigns = campaignGen.getRelationBuffer().getByteBuffer();
			campaigns.flip();
			
			/* Define the number of threads that will be used for generation*/
			int numberOfGeneratorThreads = 2;
			
			/* Generate input stream */
			int bufferSize = 2 * 1048576;
			coreToBind++;			
			Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind);
	    				
			
			@SuppressWarnings("unused")
			long dataSent = 0;
			// Generate data in-memory and send them with network
			
			channel.socket().setSendBufferSize(bufferSize);
			
			// Send Campaigns before generating data
			while(campaigns.hasRemaining())
				channel.write(campaigns);									
			
			while (true) {				
				GeneratedBuffer b = generator.getNext();
				//System.out.println(String.format("[DBG] %6d bytes created", b.getBuffer().capacity()));

				while (b.getBuffer().hasRemaining())
					dataSent += channel.write(b.getBuffer());
				
				b.getBuffer().clear();
				
				//System.out.println(String.format("[DBG] %6d bytes sent", dataSent));
				b.unlock();
				//Thread.sleep(10);
			}
			
			
		} catch (Exception e) {
			System.err.println(String.format("error: %s", e.getMessage()));
			e.printStackTrace();
			System.exit(1);
		}
	}
}


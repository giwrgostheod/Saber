package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.client;

import java.net.InetSocketAddress;
import java.net.InetAddress;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class InputStreamClient {
	
	private static final String usage = "usage: java InputStreamClient";
	
	public static void main (String[] args) {
		
		String hostname = "localhost";
		int port = 6667;
		
		int reports = 5000000; // 80000000;
		int tupleSize = 128;
		
		int _BUFFER_ = tupleSize * reports;
		ByteBuffer data = ByteBuffer.allocate(_BUFFER_);
		
		int bundle = 512;
		
		int L = 1;
		
		String filename = "datafile20seconds.dat";
		
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
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("-h")) { 
				hostname = args[j];
			} else
			if (args[i].equals("-p")) { 
				port = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-b")) { 
				bundle = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-L")) { 
				L = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("-f")) { 
				filename = args[j];
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}
		
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
			f = new FileInputStream("/home/george/Calcite-Saber/"+filename);
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
			}
			
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
				for (i = 0; i < L; i++) {
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


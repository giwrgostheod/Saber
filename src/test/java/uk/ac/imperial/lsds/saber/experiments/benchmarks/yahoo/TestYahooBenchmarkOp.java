package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

//import com.google.common.collect.Multimap;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.YahooBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.InputStreamGenerator;
import uk.ac.imperial.lsds.saber.processors.HashMap;

public class TestYahooBenchmarkOp {
	
	public static final String usage = "usage: TestYahooBenchmarkOp with in-memory data generation along with the application";
		
	public static ITupleSchema createInputStreamSchema () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* Event Time Timestamp:	long */
		offsets[1] =  8; /* User Id:   				uuid */		
		offsets[2] = 24; /* Page Id: 				uuid */
		offsets[3] = 40; /* Ad Id:   				uuid */  
		offsets[4] = 56; /* Ad Type:                 int */  // (0, 1, 2, 3, 4): ("banner", "modal", "sponsored-search", "mail", "mobile") 
		                                                     // => 16 bytes required if UTF-8 encoding is used
		offsets[5] = 60; /* Event Type:              int */  // (0, 1, 2)      : ("view", "click", "purchase")
		                                                     // => 8 bytes required if UTF-8 encoding is used
		offsets[6] = 64; /* IP Address:              int */  // 255.255.255.255 => Either 4 bytes (IPv4) or 16 bytes (IPv6)
		
		/* Additional fields to simulate the strings*/
		//offsets[7] = 76; /*                        long */ // 8 more bytes for the ad_type column		
		
		//offsets[8] = 84; /*                         int */ // 4 more bytes for the event_type column
		
		//offsets[9] = 88; /*                        long */ // 8 more bytes for ip_address column 
				
		ITupleSchema schema = new TupleSchema (offsets, 68);
		
		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
		schema.setAttributeType (0, PrimitiveType.LONG 	   );
		schema.setAttributeType (1, PrimitiveType.LONGLONG );
		schema.setAttributeType (2, PrimitiveType.LONGLONG );
		schema.setAttributeType (3, PrimitiveType.LONGLONG );
		schema.setAttributeType (4, PrimitiveType.INT  	   );
		schema.setAttributeType (5, PrimitiveType.INT      );
		schema.setAttributeType (6, PrimitiveType.INT      );
/*		schema.setAttributeType (7, PrimitiveType.LONG     );
		schema.setAttributeType (8, PrimitiveType.INT      );
		schema.setAttributeType (9, PrimitiveType.LONG     );*/
		
		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "user_id");
		schema.setAttributeName (2, "page_id");
		schema.setAttributeName (3, "ad_id");
		schema.setAttributeName (4, "ad_type");
		schema.setAttributeName (5, "event_type");
		schema.setAttributeName (6, "ip_address");	
		
		//schema.setName("InputStream");
		return schema;
	}
	
	public static void main(String [] args) throws Exception {
		
        //================================================================================
		/* Parse command line arguments */
		int numberOfThreads = 1;
		int batchSize = 1048576;
		String executionMode = "cpu";
		int tuplesPerInsert = 1024; 	// recordsPerSecond =  5000000 or 80000000
		int circularBufferSize = 64 * 1048576;
		int unboundedBufferSize = 1 * 1048576;
		int hashTableSize = 8 * 65536; // 1 * 1048576 / 256; //8 * 65536;
		int partialWindows = 4; // 64; // 1048576;
		int adsPerCampaign = 10;
		boolean realtime = true;
		int windowSize = realtime? 10000 : 10000000;
		
		
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
			if (args[i].equals("--tuples-per-insert")) { 
				tuplesPerInsert = Integer.parseInt(args[j]);
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
			}else
			if (args[i].equals("--ads-per-campaign")) { 
				adsPerCampaign = Integer.parseInt(args[j]);
			}  else {
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

		
		/* Create Input Schema */
		ITupleSchema inputSchema = createInputStreamSchema();
		
				
		//================================================================================
		// FILTER (event_type == "view")
		/* Create the predicates required for the filter operator */
		IPredicate selectPredicate = new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(5), new IntConstant(0));
		//================================================================================
		
		
		
		//================================================================================
		// PROJECT (ad_id, event_time).
		/* Define which fields are going to be projected */
		Expression [] expressions = new Expression [2];
		expressions[0] = new LongColumnReference(0);  	   // event_time
		expressions[1] = new LongLongColumnReference (3);  // ad_id	
		//================================================================================

		
		
		//================================================================================
		// JOIN on ad_id with Relational Table
		IPredicate joinPredicate = new LongLongComparisonPredicate
				(LongLongComparisonPredicate.EQUAL_OP , new LongLongColumnReference(1), new LongLongColumnReference(0));
				
		/* Generate Campaigns' ByteBuffer and HashTable */
		// WindowHashTable relation = CampaignGenerator.generate();
		CampaignGenerator campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate);	
		ITupleSchema relationSchema = campaignGen.getCampaignsSchema();
		IQueryBuffer relationBuffer = campaignGen.getRelationBuffer();
		//Multimap<Integer, Integer> multimap = campaignGen.getHashTable();
		HashMap hashMap = campaignGen.getHashMap();
		//================================================================================
		
		
		
		//================================================================================
		// AGGREGATE (count("*") as count, max(event_time) as 'lastUpdate) 
		// GROUP BY Campaign_ID with 10 seconds tumbling window
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.ROW_BASED, windowSize, windowSize);
		AggregationType [] aggregationTypes = new AggregationType [2];

		System.out.println("[DBG] aggregation type is COUNT(*)" );
		aggregationTypes[0] = AggregationType.CNT;
		System.out.println("[DBG] aggregation type is MAX(0)" );
		aggregationTypes[1] = AggregationType.MAX;
		
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [2];
		aggregationAttributes[0] = new FloatColumnReference(1);
		aggregationAttributes[1] = new FloatColumnReference(0);
		
		Expression [] groupByAttributes = new Expression [] {new LongLongColumnReference(3)};
		//================================================================================
		
		
		
		//================================================================================
		/* Generate input stream */
		long[][] ads = campaignGen.getAds();
		InputStreamGenerator gen = new InputStreamGenerator(inputSchema, adsPerCampaign, tuplesPerInsert, ads, false);
		ByteBuffer buffer = gen.generateNext(realtime);
		long timestampReference = gen.getTimestampReference();
		//================================================================================
		
		
		
		//================================================================================
        // Create and initialize the operator for computing the Benchmark's data.		
		IOperatorCode cpuCode = new YahooBenchmarkOp (
				inputSchema, 
				selectPredicate, 
				expressions, 
				joinPredicate, 
				relationSchema,
				relationBuffer,
				//multimap,
				hashMap,
				windowDefinition, null, null, null
				//aggregationTypes, 
				//aggregationAttributes, 
				//groupByAttributes
				);
		
		IOperatorCode gpuCode = null;
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);				
		
		Query query1 = new Query (0, operators, inputSchema, windowDefinition, null, null, queryConf, timestampReference);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query1);
		
		
		// Create the aggregate operator
		ITupleSchema joinSchema = ((YahooBenchmarkOp) cpuCode).getOutputSchema();
		cpuCode = new Aggregation (windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
		operator = new QueryOperator (cpuCode, gpuCode);
		operators = new HashSet<QueryOperator>();
		operators.add(operator);
		Query query2 = new Query (1, operators, joinSchema, windowDefinition, null, null, queryConf, timestampReference);
		queries.add(query2);
		query1.connectTo(query2);
		// ...
		
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		/* The path is query -> dispatcher -> handler -> aggregator */
		if (SystemConf.CPU)
			query2.setAggregateOperator((IAggregateOperator) cpuCode);
		else
			query2.setAggregateOperator((IAggregateOperator) gpuCode);
		//================================================================================
		
		
		
		//================================================================================
		/* Start execution*/
		try {
			while (true) {
				application.processData (buffer.array());

				/* Control the input rate */
				// Thread.sleep(1000);
				
				/* Generate more data */
				/*
				for (i = 0; i < buffer.position(); i+= inputSchema.getTupleSize()) {
					buffer.putLong(i, );
				}
				*/
				
				/* Update timestamps */
				//buffer = gen.updateTimestamps(realtime);
				
				//buffer = gen.generateNext();
				
				if (SystemConf.LATENCY_ON)
					buffer.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), buffer.getLong(0)));
			}
		} catch (Exception e) { 
			e.printStackTrace(); 
			System.exit(1);
		}
		//================================================================================
		
	}
}

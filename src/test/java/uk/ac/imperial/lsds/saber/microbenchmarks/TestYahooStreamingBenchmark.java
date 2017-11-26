package uk.ac.imperial.lsds.saber.microbenchmarks;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

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
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IFragmentWindowsOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.HashJoin;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class TestYahooStreamingBenchmark {
	
	public static final String usage = "usage: TestYahooStreamingBenchmark";
		
	// recordsPerSecond =  5000000
	// recordsPerSecond = 80000000
	public static ITupleSchema createInputStreamSchema () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* Event Time Timestamp:	long */
		offsets[1] =  8; /* User Id:   				 int */ 
		offsets[2] = 12; /* Page Id: 				 int */
		offsets[3] = 16; /* Ad Id:   				 int */  
		offsets[4] = 20; /* Ad Type:   				 int */  // (0, 1, 2, 3, 4): ("banner", "modal", "sponsored-search", "mail", "mobile")
		offsets[5] = 24; /* Event Type:   			 int */  // (0, 1, 2)      : ("view", "click", "purchase")
		offsets[6] = 28; /* IP Address:   			 int */
				
		ITupleSchema schema = new TupleSchema (offsets, 32);
		
		/* 0:undefined 1:int, 2:float, 3:long */
		schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (1, PrimitiveType.INT  );
		schema.setAttributeType (2, PrimitiveType.INT  );
		schema.setAttributeType (3, PrimitiveType.INT  );
		schema.setAttributeType (4, PrimitiveType.INT  );
		schema.setAttributeType (5, PrimitiveType.INT  );
		schema.setAttributeType (6, PrimitiveType.INT  );
		
		schema.setAttributeName (0, "event_time");
		schema.setAttributeName (1, "user_id");
		schema.setAttributeName (2, "page_id");
		schema.setAttributeName (3, "ad_id");
		schema.setAttributeName (4, "ad_type");
		schema.setAttributeName (5, "event_type");
		schema.setAttributeName (6, "ip_address");	
		
		//schema.setName("InputStream");
		return schema;
	}

	//100 = The number of campaigns to generate events for
	public static ITupleSchema createCampaignsSchema () {
		
		int [] offsets = new int [3];
		
		offsets[0] =  0; /* Timestamp:	   long */
		offsets[1] =  8; /* Ad Id:   	 	int */ 
		offsets[2] = 12; /* Campaign Id:	int */
				
		ITupleSchema schema = new TupleSchema (offsets, 16);
		
		/* 0:undefined 1:int, 2:float, 3:long */
		schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (1, PrimitiveType.INT  );
		schema.setAttributeType (2, PrimitiveType.INT  );
		
		schema.setAttributeName (0, "timestamp");
		schema.setAttributeName (1, "ad_id");
		schema.setAttributeName (2, "campaign_id");
		
		//schema.setName("Campaigns");
		return schema;
	}

	public static void main(String [] args) {
	
	    //================================================================================
		// Configuration of SABER with either input arguments or default values. 
		String executionMode = "cpu";
		int numberOfThreads = 1;
		int batchSize = 1048576;
		WindowType joinWindowType = WindowType.ROW_BASED;
		int joinWindowRange = 1024;
		int joinWindowSlide = 1024;
		int selectivity = 1;
		int tuplesPerInsert = 5000000;	
		
		int circularBufferSize = 256 * 1048576;
		int unboundedBufferSize = 16 * 1048576;
		int hashTableSize = 1048576;
		int partialWindows = 1048576;
		
		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
		SystemConf.LATENCY_ON = false;

		
		/* Parse command line arguments */
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
			if (args[i].equals("--window-type-of-join")) { 
				joinWindowType = WindowType.fromString(args[j]);
			} else
			if (args[i].equals("--window-range-of-join")) { 
				joinWindowRange = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--window-slide-of-join")) { 
				joinWindowSlide = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--selectivity")) { 
				selectivity = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--tuples-per-insert")) { 
				tuplesPerInsert = Integer.parseInt(args[j]);
			}else
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
		
		SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;
		
		SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;
		
		SystemConf.HASH_TABLE_SIZE = hashTableSize;
		
		SystemConf.PARTIAL_WINDOWS = partialWindows;
		
		SystemConf.SWITCH_THRESHOLD = 10;
		
		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;
		
		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
		
		SystemConf.CPU = false;
		SystemConf.GPU = false;
		
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;			
		
		QueryConf queryConf = new QueryConf (batchSize);
		Set<Query> queries = new HashSet<Query>();
		long timestampReference = System.nanoTime();
	    //================================================================================

		
		
	    //================================================================================
		// Create input schema.
		ITupleSchema  inputSchema = createInputStreamSchema();
		/* Reset tuple size */
		int inputStreaTupleSize = inputSchema.getTupleSize();
	    //================================================================================

		

		
	    //================================================================================
		// Filter (event_type == "view"). 		
		WindowDefinition filterWindow = new WindowDefinition (WindowType.ROW_BASED, 1, 1);			
		IPredicate predicate = new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(1), new IntConstant(0));
		
		IOperatorCode cpuCode = new Selection (predicate);
		IOperatorCode gpuCode = new SelectionKernel (inputSchema, predicate, null, queryConf.getBatchSize());
		
		QueryOperator filterOperator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(filterOperator);				
		Query filterQuery = new Query (0, operators, inputSchema, filterWindow, null, null, queryConf, timestampReference);						
		queries.add(filterQuery);
	    //================================================================================

		
		
	    //================================================================================
		// Project (ad_id, event_time).
		WindowDefinition projectWindow = new WindowDefinition (WindowType.ROW_BASED, 1, 1);		
		ITupleSchema projectSchema = inputSchema;
		
		Expression [] expressions = new Expression [2];
		expressions[0] = new LongColumnReference(0); // event_time
		expressions[1] = new IntColumnReference (3);  // ad_id		
		
		cpuCode = new Projection (expressions);
		gpuCode = new ProjectionKernel (projectSchema, expressions, batchSize, 0);
		
		QueryOperator projectOperator = new QueryOperator (cpuCode, gpuCode);
		operators = new HashSet<QueryOperator>();			
		operators.add(projectOperator);
		Query projectQuery = new Query (1, operators, projectSchema, projectWindow, null, null, queryConf, timestampReference);						
		queries.add(projectQuery);

		filterQuery.connectTo(projectQuery);
	    //================================================================================

		
		
	    //================================================================================
		// Join on ad_id with Relational Table
						
		ITupleSchema joinRelationalSchema = createCampaignsSchema();
		/* Reset tuple size */
		int campaignsTupleSize = joinRelationalSchema.getTupleSize();
		
		// set the size of the relational table
		SystemConf.RELATIONAL_TABLE_BUFFER_SIZE = campaignsTupleSize * tuplesPerInsert;
		// set the window on the relational table correctly
		// WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
		WindowDefinition windowRelational = new WindowDefinition (WindowType.ROW_BASED, tuplesPerInsert, tuplesPerInsert);
		
		
		ITupleSchema joinStreamSchema = projectQuery.getSchema();
		/* Reset tuple size */
		int streamTupleSize = joinStreamSchema.getTupleSize();
		WindowDefinition windowStream = new WindowDefinition (joinWindowType, joinWindowRange, joinWindowSlide);

		
		predicate =  new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP , new IntColumnReference(1), new IntColumnReference(1));
		
		
		cpuCode = new HashJoin (joinStreamSchema, joinRelationalSchema, predicate); //ThetaJoin (joinRelationalSchema, joinStreamSchema, predicate);
		gpuCode = new ThetaJoinKernel (joinStreamSchema, joinRelationalSchema, predicate, null, batchSize, 1048576);
		QueryOperator joinOperator = new QueryOperator (cpuCode, gpuCode);
		operators = new HashSet<QueryOperator>();			
		operators.add(joinOperator);							
			
		// the Boolean parameters are required to define if a relational table is used or not
		Query joinQuery = new Query (2, operators, joinStreamSchema, windowStream, false, joinRelationalSchema, windowRelational, true, queryConf, timestampReference);

		queries.add(joinQuery);
		joinQuery.connectTo(filterQuery);			
	    //================================================================================

		
		
	    //================================================================================
		// Aggregate (count("*") as count, max(event_time) as 'lastUpdate) 
		// Group By Campaign_ID with 10 seconds tumbling window
		WindowDefinition aggregateWindow = new WindowDefinition (WindowType.RANGE_BASED , 10000, 10000);	
		ITupleSchema aggregateSchema = joinQuery.getSchema();
		int aggregateStreamTupleSize = aggregateSchema.getTupleSize();
		
		AggregationType [] aggregationTypes = new AggregationType [2];

		System.out.println("[DBG] aggregation type is COUNT(*)" );
		aggregationTypes[0] = AggregationType.CNT;
		System.out.println("[DBG] aggregation type is MAX(0)" );
		aggregationTypes[1] = AggregationType.MAX;
		
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [2];
		for (i = 0; i < 2; ++i)
			aggregationAttributes[i] = new FloatColumnReference(0);
		
		Expression [] groupByAttributes = new Expression [] {new IntColumnReference(2)};
		
		//fix the aggregationTypes implementation to fit to our query
		cpuCode = new Aggregation (aggregateWindow, aggregationTypes, aggregationAttributes, groupByAttributes);
		gpuCode = new AggregationKernel (aggregateWindow, aggregationTypes, aggregationAttributes, groupByAttributes, aggregateSchema, batchSize);
		
		QueryOperator aggregateOperator = new QueryOperator (cpuCode, gpuCode);
		
		operators = new HashSet<QueryOperator>();
		operators.add(aggregateOperator);
		
		Query aggregateQuery = new Query (3, operators, aggregateSchema, aggregateWindow, null, null, queryConf, timestampReference);
				
		queries.add(aggregateQuery);
		
		aggregateQuery.connectTo(joinQuery);
	    //================================================================================

		
		
	    //================================================================================
		// Set up application and initialize the dispatchers for aggregate and hash join operators.
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		
		if (SystemConf.CPU) {
			joinQuery.setFragmentWindowsOperator((IFragmentWindowsOperator) cpuCode, true);
			aggregateQuery.setFragmentWindowsOperator((IFragmentWindowsOperator) cpuCode, false);
		} else {
			joinQuery.setFragmentWindowsOperator((IFragmentWindowsOperator) gpuCode, true);
			aggregateQuery.setFragmentWindowsOperator((IFragmentWindowsOperator) gpuCode, false);
		}
	    //================================================================================

		
		
	    //================================================================================
		/* Set up the input streams */
		
		byte [] data1 = new byte [inputStreaTupleSize * tuplesPerInsert];
		byte [] data2 = new byte [campaignsTupleSize * 100];
		
		ByteBuffer b1 = ByteBuffer.wrap(data1);
		ByteBuffer b2 = ByteBuffer.wrap(data2);
		
		/* Fill the buffers */
		
		// Buffer related to the input Stream
		int value = 0;
		int timestamp = 0;
		while (b1.hasRemaining()) {
			timestamp ++;
			b1.putLong (timestamp);
			b1.putInt(selectivity);			
			for (i = 1; i < 7; i++)
				b1.putInt(1);
		}
		
		// Buffer related to the Table of Campaigns
		timestamp = 0;
		while (b2.hasRemaining()) {
			timestamp ++;
			value = (value + 1) % 100; 
			b2.putLong (timestamp);
			if (value == 4 || value == 17)
				b2.putInt(1);
			else
				b2.putInt(value);
			for (i = 1; i < 2; i++)
				b2.putInt(2);
		}
	    //================================================================================

		
		
	    //================================================================================
		// Start Execution ...
		
		/* Reset timestamp */
		if (SystemConf.LATENCY_ON) {
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
			long packedTimestamp = Utils.pack(systemTimestamp, b1.getLong(0));
			b1.putLong(0, packedTimestamp);
		}
		
		try {
			application.processSecondStream  (joinQuery.getId(), data2); // fill the relational table's buffer only once!
			while (true) {	
	            Thread.sleep(1000); // control the input rate
				application.processFirstStream (data1);				
				if (SystemConf.LATENCY_ON)
					b1.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), b1.getLong(0)));
			}
		} catch (Exception e) { 
			e.printStackTrace(); 
			System.exit(1);
		}
	    //================================================================================

	}
}

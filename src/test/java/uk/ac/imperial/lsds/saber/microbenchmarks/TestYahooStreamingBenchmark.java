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
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;
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
	
		String executionMode = "cpu";
		int numberOfThreads = 1;
		int batchSize = 1048576;
		WindowType windowType1 = WindowType.ROW_BASED;
		int windowRange1 = 1024;
		int windowSlide1 = 1024;
		int numberOfAttributes1 = 6;
		WindowType windowType2 = WindowType.ROW_BASED;
		int windowRange2 = 1024;
		int windowSlide2 = 1024;
		int numberOfAttributes2 = 6;
		int selectivity = 1;
		int tuplesPerInsert = 128;		
		int i;
		
		SystemConf.CIRCULAR_BUFFER_SIZE = 32 * 1048576;
		
		SystemConf.UNBOUNDED_BUFFER_SIZE = 16 * 1048576;	
		
		SystemConf.CPU = true;
		SystemConf.GPU = false;		
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;				
		
		QueryConf queryConf = new QueryConf (batchSize);
		Set<Query> queries = new HashSet<Query>();
		long timestampReference = System.nanoTime();
		
		// Create input schema.
		ITupleSchema  inputSchema = createInputStreamSchema();
		/* Reset tuple size */
		int tupleSize = inputSchema.getTupleSize();
		
		
		
		// -----------------------------
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
		// -----------------------------

		
		
		// -----------------------------
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
		Query projectQuery = new Query (0, operators, projectSchema, projectWindow, null, null, queryConf, timestampReference);						

		filterQuery.connectTo(projectQuery);
		// -----------------------------

		
		
		// -----------------------------------
		// Join on ad_id with Relational Table
						
		ITupleSchema joinRelationalSchema = createCampaignsSchema();
		/* Reset tuple size */
		int tupleSize1 = joinRelationalSchema.getTupleSize();
		
		// set the size of the relational table
		SystemConf.RELATIONAL_TABLE_BUFFER_SIZE = tupleSize1 * tuplesPerInsert;
		// set the window on the relational table correctly
		// WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
		WindowDefinition window1 = new WindowDefinition (WindowType.ROW_BASED, tuplesPerInsert, tuplesPerInsert);
		
		
		ITupleSchema joinStreamSchema = projectQuery.getSchema();
		/* Reset tuple size */
		int tupleSize2 = joinStreamSchema.getTupleSize();
		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);

		
		predicate =  new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP , new IntColumnReference(1), new IntColumnReference(1));
		
		cpuCode = new ThetaJoin (joinRelationalSchema, joinStreamSchema, predicate);
		gpuCode = new ThetaJoinKernel (joinRelationalSchema, joinStreamSchema, predicate, null, batchSize, 1048576);
		
		QueryOperator joinOperator = new QueryOperator (cpuCode, gpuCode);
		
		operators = new HashSet<QueryOperator>();
		operators.add(joinOperator);
			
		// the Boolean parameters are required to define if a relational table is used or not
		Query joinQuery = new Query (0, operators, joinRelationalSchema, window1, true, joinStreamSchema, window2, false, queryConf, timestampReference);
				
		queries.add(joinQuery);
		joinQuery.connectTo(filterQuery);
		// -----------------------------

		
		// -----------------------------------
		// Aggregate (count("*") as count, max(event_time) as 'lastUpdate) 
		// Group By Campaign_ID with 10 seconds tumbling window
		WindowDefinition aggregateWindow = new WindowDefinition (WindowType.RANGE_BASED , 10000, 10000);	
		ITupleSchema aggregateSchema = joinQuery.getSchema();
		tupleSize = aggregateSchema.getTupleSize();
		
		AggregationType [] aggregationTypes = new AggregationType [2];

		System.out.println("[DBG] aggregation type is COUNT(*)" );
		aggregationTypes[0] = AggregationType.CNT;
		System.out.println("[DBG] aggregation type is MAX(0)" );
		aggregationTypes[2] = AggregationType.MAX;
		
		LongColumnReference [] aggregationAttributes = new LongColumnReference [2];
		for (i = 0; i < 2; ++i)
			aggregationAttributes[i] = new LongColumnReference(0);
		
		Expression [] groupByAttributes = new Expression [] {new IntColumnReference(2)};
		
		//fix!!
		
		//cpuCode = new Aggregation (aggregateWindow)
		System.out.println(cpuCode);
		//gpuCode = new AggregationKernel (aggregateWindow, aggregationTypes, aggregationAttributes, groupByAttributes, aggregateSchema, batchSize);
		
		QueryOperator aggregateOperator = new QueryOperator (cpuCode, gpuCode);
		
		operators = new HashSet<QueryOperator>();
		operators.add(aggregateOperator);
		
		Query aggregateQuery = new Query (0, operators, aggregateSchema, aggregateWindow, null, null, queryConf, timestampReference);
		
		queries = new HashSet<Query>();
		queries.add(aggregateQuery);
		
		
		// --------------------------
		// Set up application.
		QueryApplication application = new QueryApplication(queries);
		
		application.setup();
		
		/* Set up the input streams */
		
		byte [] data1 = new byte [tupleSize1 * tuplesPerInsert];
		byte [] data2 = new byte [tupleSize2 * tuplesPerInsert];
		
		ByteBuffer b1 = ByteBuffer.wrap(data1);
		ByteBuffer b2 = ByteBuffer.wrap(data2);
		
		/* Fill the buffers */
		
		int value = 0;
		int timestamp = 0;
		while (b1.hasRemaining()) {
			timestamp ++;
			b1.putLong (timestamp);
			b1.putInt(value);
			value = (value + 1) % 100; 
			for (i = 1; i < numberOfAttributes1; i++)
				b1.putInt(1);
		}
		
		timestamp = 0;
		while (b2.hasRemaining()) {
			timestamp ++;
			b2.putLong (timestamp);
			b2.putInt(selectivity);
			for (i = 1; i < numberOfAttributes2; i++)
				b2.putInt(1);
		}
		
		/* Reset timestamp */
		if (SystemConf.LATENCY_ON) {
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
			long packedTimestamp = Utils.pack(systemTimestamp, b1.getLong(0));
			b1.putLong(0, packedTimestamp);
		}
		
		try {
			while (true) {	
	            // Thread.sleep(10);
				application.processFirstStream  (data1);
				application.processSecondStream (data2);
				if (SystemConf.LATENCY_ON)
					b1.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), b1.getLong(0)));
			}
		} catch (Exception e) { 
			e.printStackTrace(); 
			System.exit(1);
		}
	}
}

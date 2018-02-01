package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
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
import uk.ac.imperial.lsds.saber.cql.operators.cpu.NoOp;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.YahooBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.NoOpKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;
import uk.ac.imperial.lsds.saber.processors.HashMap;

// This example is the implementation of the query: SELECT * from NetworkTraffic WHERE latency > threshold
// without SQL.

public class YahooBenchmark extends InputStream {
	
	private long[][] ads;
	private int adsPerCampaign;

	
	public YahooBenchmark (QueryConf queryConf, boolean isExecuted) {
		this(queryConf, isExecuted, null);
	}
	
	public YahooBenchmark (QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns) {
		adsPerCampaign = 10;
		createSchema ();
		createApplication (queryConf, isExecuted, campaigns);
	}
	
	public void createApplication (QueryConf queryConf, boolean isExecuted) {
		this.createApplication(queryConf, isExecuted, null);
	}
	
	public void createApplication(QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns) {

		
        //================================================================================
		long timestampReference = System.nanoTime(); //Fix this
		boolean realtime = true;
		int windowSize = realtime? 10000 : 10000000;
		//================================================================================

		
		/* Create Input Schema */
		ITupleSchema inputSchema = schema;
		
				
		//================================================================================
		
/*		IPredicate [] loadpreds  = new IPredicate [100];
		for (int k = 0; k < loadpreds.length; k++)
			loadpreds[k] = new IntComparisonPredicate(IntComparisonPredicate.LESS_OP, new IntColumnReference(5), new IntConstant(4));
		IPredicate loadpred = new ANDPredicate (loadpreds);
		// JOIN on ad_id with Relational Table
		IPredicate selectPredicate = new ANDPredicate (
				 new IntComparisonPredicate
					(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(5), new IntConstant(0)),
				loadpred			
				);*/
		// FILTER (event_type == "view")
		/* Create the predicates required for the filter operator */
		IPredicate selectPredicate = new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(5), new IntConstant(0));
		//================================================================================
		
		
		
		//================================================================================
		// PROJECT (ad_id, event_time).
		/* Define which fields are going to be projected */
		Expression [] expressions = new Expression [2];
		expressions[0] = new LongColumnReference(0); 	   // event_time
		expressions[1] = new LongLongColumnReference (3);  // ad_id	
		//================================================================================

		
		
		//================================================================================
		
		IPredicate joinPredicate = new LongLongComparisonPredicate
				(LongLongComparisonPredicate.EQUAL_OP , new LongLongColumnReference(1), new LongLongColumnReference(0));
				
		/* Generate Campaigns' ByteBuffer and HashTable */
		// WindowHashTable relation = CampaignGenerator.generate();
		
		CampaignGenerator campaignGen = null;
		if (campaigns == null)
			campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate);
		else
			campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate, campaigns);

		ITupleSchema relationSchema = campaignGen.getCampaignsSchema();
		IQueryBuffer relationBuffer = campaignGen.getRelationBuffer();
		this.ads = campaignGen.getAds();
		HashMap hashMap = campaignGen.getHashMap();
		//================================================================================
		
		
		
		//================================================================================
		// AGGREGATE (count("*") as count, max(event_time) as 'lastUpdate) 
		// GROUP BY Campaign_ID with 10 seconds tumbling window
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);
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
		
		cpuCode = new NoOp ();
		IOperatorCode gpuCode = null;
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);				
		
		Query query1 = new Query (0, operators, inputSchema, windowDefinition, null, null, queryConf, timestampReference);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query1);
		
		
		// Create the aggregate operator
		/*ITupleSchema joinSchema = ((YahooBenchmarkOp) cpuCode).getOutputSchema();
		cpuCode = new Aggregation (windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
		operator = new QueryOperator (cpuCode, gpuCode);
		operators = new HashSet<QueryOperator>();
		operators.add(operator);
		Query query2 = new Query (1, operators, joinSchema, windowDefinition, null, null, queryConf, timestampReference);
		queries.add(query2);
		query1.connectTo(query2);*/
		// ...

		if (isExecuted) {
			application = new QueryApplication(queries);
			application.setup();
		
			/* The path is query -> dispatcher -> handler -> aggregator */
/*			if (SystemConf.CPU)
				query2.setAggregateOperator((IAggregateOperator) cpuCode);
			else
				query2.setAggregateOperator((IAggregateOperator) gpuCode);*/
		}
		//================================================================================
		
		return;
	}
	
	public long[][] getAds () {
		return this.ads;
	}
	
	public int getAdsPerCampaign () {
		return this.adsPerCampaign;
	}
}

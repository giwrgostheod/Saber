package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class HashJoin implements IOperatorCode {
	
	private static boolean debug = false;
	private static boolean monitorSelectivity = false;
	
	private long invoked = 0L;
	private long matched = 0L;
	
	private IPredicate predicate;

	private ITupleSchema schema1 = null;
	private ITupleSchema schema2 = null;
	private ITupleSchema outputSchema = null;
	
	private Multimap<Integer,Integer> multimap;
	private boolean isFirst = true;
	
	public HashJoin(ITupleSchema schema1, ITupleSchema schema2, IPredicate predicate) {
		
		this.predicate = predicate;
		
		this.schema1 = schema1;
		
		this.schema2 = schema2;
		
		outputSchema = ExpressionsUtil.mergeTupleSchemas(schema1, schema2);
	}
	
	public void processData(WindowBatch batch1, WindowBatch batch2, IWindowAPI api) {
		
		int column1 = ((IntColumnReference)predicate.getFirstExpression()).getColumn();
		int offset1 = schema1.getAttributeOffset(column1);
		int column2 = ((IntColumnReference)predicate.getSecondExpression()).getColumn();
		int offset2 = schema2.getAttributeOffset(column2);

		// create hash table the first time only for the first batch
		if (this.isFirst) {
			createHashTable(batch2, offset2);
			isFirst = false;
		}
		
		int currentIndex1 = batch1.getBufferStartPointer();
		int currentIndex2 = batch2.getBufferStartPointer();

		int endIndex1 = batch1.getBufferEndPointer() + 32;
		int endIndex2 = batch2.getBufferEndPointer() + 32;
		
/*		int currentWindowStart1 = currentIndex1;
		int currentWindowEnd1   = currentIndex1;
		
		int currentWindowStart2 = currentIndex2;
		int currentWindowEnd2   = currentIndex2;*/
		
		IQueryBuffer buffer1 = batch1.getBuffer();
		IQueryBuffer buffer2 = batch2.getBuffer();
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema1 = batch1.getSchema();
		ITupleSchema schema2 = batch2.getSchema();

		int tupleSize1 = schema1.getTupleSize();
		int tupleSize2 = schema2.getTupleSize();

/*		WindowDefinition windowDef1 = batch1.getWindowDefinition();
		WindowDefinition windowDef2 = batch2.getWindowDefinition();*/
		
		if (debug) {
			System.out.println(
				String.format("[DBG] t %6d batch-1 [%10d, %10d] %10d tuples [f %10d] / batch-2 [%10d, %10d] %10d tuples [f %10d]", 
					batch1.getTaskId(), 
					currentIndex1, 
					endIndex1, 
					(endIndex1 + tupleSize1 - currentIndex1)/tupleSize1,
					batch1.getFreePointer(),
					currentIndex2, 
					endIndex2,
					(endIndex2 + tupleSize2 - currentIndex2)/tupleSize2,
					batch2.getFreePointer()
				)
			);
		}
		
/*		long currentTimestamp1, startTimestamp1;
		long currentTimestamp2, startTimestamp2;*/
		
		if (monitorSelectivity)
			invoked = matched = 0L;
		
		/* Is one of the windows empty? */
		if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) { 
					
			int value;
			List<Integer> relationalBufferPointers;
			for (int pointer = batch1.getBufferStartPointer(); pointer < batch1.getBufferEndPointer(); pointer += tupleSize1) {
				value = buffer1.getInt(pointer + offset1);
				if (multimap.containsKey(value)) {
					relationalBufferPointers = (List<Integer>)multimap.get(value);
					for (int p: relationalBufferPointers) {
						/* Write tuple to result buffer */
						buffer1.appendBytesTo(pointer, tupleSize1, outputBuffer);
						buffer2.appendBytesTo(p, tupleSize2, outputBuffer);
					}
				}
			}
		}
				
		buffer1.release();
		// buffer2.release();

		batch1.setBuffer(outputBuffer);
		batch1.setSchema(outputSchema);
		
		if (debug) 
			System.out.println("[DBG] output buffer position is " + outputBuffer.position());
		
		if (monitorSelectivity) {
			double selectivity = 0D;
			if (invoked > 0)
				selectivity = ((double) matched / (double) invoked) * 100D;
			System.out.println(String.format("[DBG] task %6d %2d out of %2d tuples selected (%4.1f)", 
					batch1.getTaskId(), matched, invoked, selectivity));
		}
		
		/*		Print tuples */
/*		outputBuffer.close();
		int tid = 1;
		while (outputBuffer.hasRemaining()) {
		
			System.out.println(String.format("%03d: %2d,%2d,%2d | %2d,%2d,%2d", 
			tid++,
		    outputBuffer.getByteBuffer().getLong(),
			outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getLong(),
			outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			//outputBuffer.getByteBuffer().getInt (),
			outputBuffer.getByteBuffer().getInt ()
			));
		}*/
		
		//Don't comment out
		api.outputWindowBatchResult(batch1);
	
/*		System.err.println("Disrupted");
		System.exit(-1);*/
	}
	
	@SuppressWarnings("unused")
	private long getTimestamp (WindowBatch batch, int index, int attribute) {
		long value = batch.getLong (index, attribute);
		if (SystemConf.LATENCY_ON)
			return (long) Utils.getTupleTimestamp(value);
		return value;
	}
	
	public void processData(WindowBatch batch, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operator on a single stream");
	}

	public void configureOutput (int queryId) {
		
		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	public void processOutput (int queryId, WindowBatch batch) {
		
		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}
	
	public void setup() {
		
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}
	
	public void createHashTable(WindowBatch batch, int offset) {
		
		this.multimap = ArrayListMultimap.create();		
		IQueryBuffer buffer = batch.getBuffer();		
		int tupleSize = schema1.getTupleSize();
		
		int endIndex = buffer.capacity(); //batch1.getBufferEndPointer();		
		int i = 0;
		while ( i <= endIndex) {
			// check if the column is float						
			multimap.put(buffer.getInt(i + offset), i);
			i += tupleSize;
		}		
	}
}

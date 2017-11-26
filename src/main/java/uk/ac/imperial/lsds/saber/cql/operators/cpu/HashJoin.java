package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IFragmentWindowsOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class HashJoin implements IOperatorCode, IFragmentWindowsOperator {
	
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
	private boolean processIncremental;
	
	public HashJoin(ITupleSchema schema1, ITupleSchema schema2, IPredicate predicate) {
		
		this.predicate = predicate;
		
		this.schema1 = schema1;
		
		this.schema2 = schema2;
		
		outputSchema = ExpressionsUtil.mergeTupleSchemas(schema1, schema2);
	}
	
	public void processData(WindowBatch batch1, WindowBatch batch2, IWindowAPI api) {

		batch1.initPartialWindowPointers();
		
		int column2 = ((IntColumnReference)predicate.getSecondExpression()).getColumn();
		int offset2 = schema2.getAttributeOffset(column2);

		// create hash table the first time only for the first batch
		if (this.isFirst) {
			createRelationalHashTable(batch2, offset2);
			isFirst = false;
		}		

		WindowDefinition windowDef1 = batch1.getWindowDefinition();
		/*WindowDefinition windowDef2 = batch2.getWindowDefinition();*/	
		
/*		long currentTimestamp1, startTimestamp1;
		long currentTimestamp2, startTimestamp2;*/
		
		processIncremental = (windowDef1.getSlide() < windowDef1.getSize() / 2);

		if (processIncremental) { 
			processDataPerWindowIncrementally (batch1, batch2, api);
		} else {
			processDataPerWindow (batch1, batch2, api);
		}
					
		//batch.getBuffer().release();
		//batch.setSchema(outputSchema);
		
		api.outputWindowBatchResult(batch1);
	}
	
	private void processDataPerWindow(WindowBatch batch1, WindowBatch batch2, IWindowAPI api) {
		
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		outputBuffer = computeOutputBuffer(batch1, batch1.getBufferStartPointer(), batch1.getBufferEndPointer(), batch2, outputBuffer);
				
		batch1.getBuffer().release();
		// buffer2.release();

		batch1.setBuffer(outputBuffer);
		batch1.setSchema(outputSchema);
	}
	
	private void processDataPerWindowIncrementally(WindowBatch batch1, WindowBatch batch2, IWindowAPI api) {
		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());
		
		int [] startP = batch1.getWindowStartPointers();
		int []   endP = batch1.getWindowEndPointers();		
		
		int numberOfAttributes = schema1.numberOfAttributes();
		
		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);
		
		IQueryBuffer outputBuffer = null;
		
		/* Current window start and end pointers */ 
		int start, end;
		/* Previous window start and end pointers */
		int _start = -1;
		int   _end = -1;
				
		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch1.getLastWindowIndex())
				break;
			
			start = startP [currentWindow];
			end   = endP   [currentWindow];
			
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch1.getBufferStartPointer();
				end = batch1.getBufferEndPointer();
				if (batch1.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();					
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch1.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch1.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;
			
			if (start == end) {
				/* Store "null" (zero-valued) tuple in output buffer */
				outputBuffer.putLong(0L);
				for (int i = 0; i < numberOfAttributes; ++i) {
					outputBuffer.putInt(0);
				}				
				/* Move to next window */
				continue;
			}
			
			//float [] values = tl_values.get(); 
			//int [] counts = tl_counts.get();
			
			if (_start >= 0) {
				/* Process tuples in current window that have not been in the previous window */
				outputBuffer = computeOutputBuffer(batch1, _end, end, batch2, outputBuffer);
			} else {
				/* Process tuples in current window */
				outputBuffer = computeOutputBuffer(batch1, start, end, batch2, outputBuffer);
			}
			
			/* Process tuples in previous window that are not in current window */
			if (_start >= 0) {
				outputBuffer = computeOutputBuffer(batch1, _start, start, batch2, outputBuffer);
			}
	
			
			/* Continue with the next window */
			_start = start;
			_end = end;
		}
		
		/* At the end of processing, set window batch accordingly */
		batch1.setClosingWindows  ( closingWindows);
		batch1.setPendingWindows  ( pendingWindows);
		batch1.setCompleteWindows (completeWindows);
		batch1.setOpeningWindows  ( openingWindows);
		
		
		batch1.getBuffer().release();

		batch1.setBuffer(outputBuffer);
		batch1.setSchema(outputSchema);
	}
	
	public IQueryBuffer computeOutputBuffer (WindowBatch batch1, int ptr1, int ptr2, WindowBatch batch2, IQueryBuffer outputBuffer) {
		
		int column1 = ((IntColumnReference)predicate.getFirstExpression()).getColumn();
		int offset1 = schema1.getAttributeOffset(column1);
		int currentIndex1 =  ptr1; // batch1.getBufferStartPointer();
		int currentIndex2 = batch2.getBufferStartPointer();

		int endIndex1 = ptr2 + 32;// batch1.getBufferEndPointer() + 32;
		int endIndex2 = batch2.getBufferEndPointer() + 32;		
		
		IQueryBuffer buffer1 = batch1.getBuffer();
		IQueryBuffer buffer2 = batch2.getBuffer();
		
		ITupleSchema schema1 = batch1.getSchema();
		ITupleSchema schema2 = batch2.getSchema();

		int tupleSize1 = schema1.getTupleSize();
		int tupleSize2 = schema2.getTupleSize();
		
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
		
		if (monitorSelectivity)
			invoked = matched = 0L;
		
		/* Is one of the windows empty? */
		if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) { 
					
			int value;
			List<Integer> relationalBufferPointers;
			for (int pointer = ptr1; pointer < ptr2; pointer += tupleSize1) {
				
				if (monitorSelectivity)
					invoked ++;
				
				value = buffer1.getInt(pointer + offset1);
				
				if (multimap.containsKey(value)) {
					relationalBufferPointers = (List<Integer>)multimap.get(value);
					for (int p: relationalBufferPointers) {
						/* Write tuple to result buffer */
						buffer1.appendBytesTo(pointer, tupleSize1, outputBuffer);
						buffer2.appendBytesTo(p, tupleSize2, outputBuffer);
						if (monitorSelectivity)
							matched ++;
					}
				}
			}
		}
		
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
		}
		System.err.println("Disrupted");
		System.exit(-1);*/
		
		return outputBuffer;
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
	
	public void createRelationalHashTable(WindowBatch batch, int offset) {
		
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

	public ITupleSchema getOutputSchema() {
		return outputSchema;
	}

	public boolean hasGroupBy() {
		return false;
	}

	public int getKeyLength() {
		return 0;
	}

	public int getValueLength() {
		return 0;
	}

	public int numberOfValues() {
		return schema1.numberOfAttributes();
	}

	public AggregationType getAggregationType() {
		return null;
	}

	public AggregationType getAggregationType(int idx) {
		return null;
	}

	public boolean isHashJoin() {
		return true;
	}
	
	
}

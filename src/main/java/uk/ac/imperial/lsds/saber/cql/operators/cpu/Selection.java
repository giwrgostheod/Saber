package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class Selection implements IOperatorCode {
	
	private IPredicate predicate;
	
	public Selection (IPredicate predicate) {
		this.predicate = predicate;
	}
	
	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		s.append("Selection (");
		s.append(predicate.toString());
		s.append(")");
		return s.toString();
	}
	
	public void processData (WindowBatch batch, IWindowAPI api) {
		
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
		
		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();
		
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			
			if (predicate.satisfied (inputBuffer, schema, pointer)) {
				
				/* Write tuple to result buffer */
				inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
			}
		}
		
		inputBuffer.release();
		batch.setBuffer(outputBuffer);
		
		api.outputWindowBatchResult (batch);
	}
	
	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {
		
		throw new UnsupportedOperationException("error: operator does not operate on two streams");
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
}

package nl.tno.stormcv.operation;

import java.util.List;

import nl.tno.stormcv.model.CVParticle;

/**
 * Interface for batch operations which take a list with {@link CVParticle} objects as input instead
 * of a single object as {@link ISingleInputOperation} requires.
 * 
 * @author Corne Versloot
 *
 * @param <Output>
 */
public interface IBatchOperation<Output extends CVParticle> extends IOperation<Output> {

	public List<Output> execute(List<CVParticle> input) throws Exception;
	
}

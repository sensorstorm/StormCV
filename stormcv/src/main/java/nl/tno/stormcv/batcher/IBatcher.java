package nl.tno.stormcv.batcher;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.BatchInputBolt.History;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IBatchOperation;

/**
 * Batcher implementations are responsible to partition a provided set of {@link CVParticle} items into zero or more batches
 * of items suitable for batch processing. Batcher's get a List with received CVParticles which they must create batches for and 
 * a {@link History} reference used to remove CVParticles from when they are no longer needed (results in an ACK for that particle).
 * The overall process works as follows:
 * <ul>
 * <li> {@link BatchInputBolt} receives a CVParticle and selects the right 'group' to add it to (for example streamId)</li>
 * <li>The group is sorted based on sequence number</li>
 * <li>The sorted group and History are provided to the Batcher implementation</li>
 * <li>The batcher creates zero or more batches, removing particles from the History when they are no longer needed</li>
 * <li>Each batch is provided to the {@link IBatchOperation} implementation which results in zero or more CVParticles to be emitted</li>
 * <li>Each CVParticle is anchored on the received Particle and emitted</li>
 * <li></li>
 * </ul>
 * Implementations typically base their results on the items received (size of the set, some particular ordering etc) but it is also possible to 
 * partition the input based on external criteria like a clock (all items received within one minute) or a specific 'marker' item received. 
 * 
 * @author Corne Versloot
 *
 */
public interface IBatcher extends Serializable{

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf) throws Exception;
	
	public List<List<CVParticle>> partition(History history, List<CVParticle> currentSet);
	
}

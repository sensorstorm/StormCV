package nl.tno.stormcv.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.bolt.BatchInputBolt.History;
import nl.tno.stormcv.model.CVParticle;

/**
 * This {@link IBatcher} implementation creates batches of received {@link CVParticle} items based on their SequenceNr.
 * If the specified number of particles with the same sequenceNr have been received it will return a batch for those
 * and remove them from the history.
 * 
 * @author Corne Versloot
 *
 */
public class SequenceNrBatcher implements IBatcher{

	private static final long serialVersionUID = 105240619435757422L;
	private int size;
	
	public SequenceNrBatcher(int size){
		this.size = size;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf) throws Exception { }

	@Override
	public List<List<CVParticle>> partition(History history, List<CVParticle> currentSet) {
		List<List<CVParticle>> result = new ArrayList<List<CVParticle>>();
		List<CVParticle> items = new ArrayList<CVParticle>();
		items.addAll(currentSet);
		if(items.size() == size){
			for(CVParticle st : items) history.removeFromHistory(st);
			result.add(items);
		}
		return result;
	}

}

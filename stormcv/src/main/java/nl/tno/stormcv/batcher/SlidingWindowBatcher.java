package nl.tno.stormcv.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.bolt.BatchInputBolt.History;
import nl.tno.stormcv.model.CVParticle;

/**
 * A {@link IBatcher} implementation that partitions a set of {@link CVParticle} items into zero or more sliding windows of the specified size.
 * The windowSize and sequenceDelta are used to determine if the provided list with items contains any sliding windows. If the 
 * Batcher was created with a windowSize of 3 and sequenceDelta of 10 the input <i>with sequenceNr's</i> [20, 30, 40, 50, 70, 80, 90] will
 * result in two 'windows': [20, 30, 40] and [30, 40, 50]. Note that no other windows are created because some item (60) is still missing.  
 * The first item of any approved window will be removed from the history because they will not be needed any more. In the example above this
 * results in the removal of item '20' and item '30' from the History which will trigger an ACK on those items.
 * 
 * If a specific element is missing this Batcher will never provide any results. To avoid this situation it is possible to set the maximum
 * number of elements in the queue. If this maximum is reached this Batcher will generate results even if they do not match the criteria specified.  
 * 
 * @author Corne Versloot
 *
 */
public class SlidingWindowBatcher implements IBatcher{

	private static final long serialVersionUID = -4296426517808304248L;
	private int windowSize;
	private int sequenceDelta;
	private int maxSize = Integer.MAX_VALUE;

	public SlidingWindowBatcher(int windowSize, int sequenceDelta){
		this.windowSize = windowSize;
		this.sequenceDelta = sequenceDelta;
	}
	
	public SlidingWindowBatcher maxSize(int size){
		this.maxSize = size;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) throws Exception {	}

	@Override
	public List<List<CVParticle>> partition(History history, List<CVParticle> currentSet) {
		List<List<CVParticle>> result = new ArrayList<List<CVParticle>>();
		for(int i=0; i<=currentSet.size()-windowSize; i++){
			List<CVParticle> window = new ArrayList<CVParticle>();
			window.addAll(currentSet.subList(i, i+windowSize)); // add all is used to avoid ConcurrentModificationException when the History cleans stuff up
			if(assessWindow(window) || currentSet.size() > maxSize){
				result.add(window);
				history.removeFromHistory(window.get(0));
			} else break;
		}
		return result;
	}

	/**
	 * Checks if the provided window fits the required windowSize and sequenceDelta criteria
	 * @param window
	 * @return
	 */
	private boolean assessWindow(List<CVParticle> window){
		if(window.size() != windowSize) return false;
		long previous = window.get(0).getSequenceNr();
		for(int i=1; i<window.size(); i++){
			if(window.get(i).getSequenceNr() - previous != sequenceDelta) return false;
			previous = window.get(i).getSequenceNr();
		}
		return true;
	}
	
}

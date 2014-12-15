package nl.tno.stormcv.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.bolt.BatchInputBolt.History;
import nl.tno.stormcv.model.CVParticle;

/**
 * A {@link IBatcher} implementation that partitions a set of {@link CVParticle} items into zero or more non overlapping batches of the specified size.
 * The windowSize and sequenceDelta are used to determine if the provided list with items contains any valid bathes. If the 
 * Batcher was created with a windowSize of 3 and sequenceDelta of 10 the input <i>with sequenceNr's</i> [20, 30, 40, 50, 60, 70, 90, 100, 110] will
 * result in two 'windows': [20, 30, 40] and [50, 60, 70]. Note that [90, 100, 110] is not a valid batch because of the missing item (80). 
 * All items from a batch will be removed from the history which will trigger an ACK for all of them.
 * 
 * This Batcher starts to be greedy when the number of input items it gets exceeds 5 * windowSize. In this case it will skip the first item
 * in the list and try to make a valid batch out of the next items. It keeps skipping until a valid batch can be created of the end of the input
 * is reached. Hence if windowSize is 2 and sequenceDelta is 10 the set [0, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110] will result in one batch 
 * containing [20, 30] (matches the criteria but skips the first element). This method avoid a bottleneck in the topology in case an expected item
 * never appears (might have been lost in preceding bolts).
 * 
 * @author Corne Versloot
 *
 */
public class DiscreteWindowBatcher implements IBatcher {

	private static final long serialVersionUID = 789734984349506398L;

	private int windowSize;
	private int sequenceDelta;
	
	public DiscreteWindowBatcher(int windowSize, int sequenceDelta){
		this.windowSize = windowSize;
		this.sequenceDelta = sequenceDelta;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf) throws Exception { }

	@Override
	public List<List<CVParticle>> partition(History history, List<CVParticle> currentSet) {
		List<List<CVParticle>> result = new ArrayList<List<CVParticle>>();
		if(currentSet.size() < windowSize) return result;

		int offset = 0;
		while(true){
			if(offset + result.size()*windowSize + windowSize > currentSet.size()) break;
			List<CVParticle> window = new ArrayList<CVParticle>();
			window.addAll(currentSet.subList(offset + result.size()*windowSize, result.size()*windowSize + windowSize + offset));
			// add window to the results if it is valid
			if(assessWindow(window)){
				result.add(window);
			}else {
				// if the current set is large enough start a greedy approach by using the offset
				if(currentSet.size()-result.size()*windowSize > 5*windowSize){
					offset++;
				}else break;
			}
			if(result.size() > 0 && offset > 0) break;
		}
		// remove the items in the selected windows from the history
		for(List<CVParticle> window : result){
			for(int i=0; i<window.size(); i++){
				history.removeFromHistory(window.get(i));
			}
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

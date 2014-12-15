package nl.tno.stormcv.grouping;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.TilesRecombinerOp;
import nl.tno.stormcv.operation.TilingOp;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

/**
 * A CutomGrouping specially designed for the routing of 'tiles' after they have been analyzed. This grouping
 * will change the streamId back to its original form (as they were before {@link TilingOp} created the tiles) and will
 * select the target task using hasmod on streamId and sequenceNr combination. This will ensure that all tiles from
 * the same stream and framenumber will be routed to the same target task.
 * 
 * Note: this grouping should only be used in combination with the {@link TilesRecombinerOp} to make sure the
 * merger gets the right tiles.
 * 
 * @author Corne Versloot
 *
 */
public class TileGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -8094480457406082990L;
	private int streamIdIndex;
	private int sequenceNrIndex;
	private List<Integer> targetTasks;
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		
		Fields tupleFields = context.getComponentOutputFields(stream);
		for(int i=0; i<tupleFields.size(); i++){
			if(tupleFields.get(i).equals(CVParticleSerializer.STREAMID)){
				streamIdIndex = i;
			}else if(tupleFields.get(i).equals(CVParticleSerializer.SEQUENCENR)){
				sequenceNrIndex = i;
			}
		}
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String streamId = (String)values.get(streamIdIndex);
		Long sequence = (Long)values.get(sequenceNrIndex);
		streamId = streamId.substring(0, streamId.lastIndexOf('_'));
		values.set(streamIdIndex, streamId); // replace the original streamId with the 'untiled' one
		
		// perform hashmod on the stream + sequence combination
		int hash = streamId.hashCode() + sequence.hashCode();
		int targetId = targetTasks.get(hash % targetTasks.size());
		List<Integer> result = new ArrayList<Integer>();
		result.add(targetId);
		return result;
	}


}

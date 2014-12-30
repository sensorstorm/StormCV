package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.GroupOfFramesSerializer;

/**
 * A {@link IBatchOperation} that simply puts all {@link Frame} objects it gets within the execute method
 * in a single {@link GroupOfFrames} object. This can be useful when a set of (subsequent) frames need to be analyzed
 * by a specific bolt.
 * 
 * @author Corne Versloot
 *
 */
public class FrameGrouperOp implements IBatchOperation<GroupOfFrames> {

	private static final long serialVersionUID = -5050491641670284538L;


	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context)	throws Exception { }

	@Override
	public void deactivate() { }

	@Override
	public CVParticleSerializer<GroupOfFrames> getSerializer() {
		return new GroupOfFramesSerializer();
	}

	@Override
	public List<GroupOfFrames> execute(List<CVParticle> input) throws Exception {
		List<Frame> frames = new ArrayList<Frame>();
		for(CVParticle particle : input){
			if(particle instanceof Frame){
				frames.add((Frame)particle);
			}
		}
		List<GroupOfFrames> result = new ArrayList<GroupOfFrames>();
		if(frames.size() > 0) result.add( new GroupOfFrames(frames.get(0).getRequestId(), frames.get(0).getStreamId(), frames.get(0).getSequenceNr(), frames) );
		return result;
	}


}

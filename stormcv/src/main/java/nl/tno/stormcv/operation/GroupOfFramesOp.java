package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.batcher.IBatcher;
import nl.tno.stormcv.fetcher.FileFrameFetcher;
import nl.tno.stormcv.fetcher.StreamFrameFetcher;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * A special {@link ISingleInputOperation} that executes a {@link IBatchOperation} on {@link Frame} objects contained in a single received {@link GroupOfFrames}.
 * This enables the platform to execute batch operations as if they are stateless and does not require a {@link IBatcher}.
 * {@link GroupOfFrames} can be created by the {@link FrameGrouperOp} or directly emitted by the {@link FileFrameFetcher} or {@link StreamFrameFetcher}
 * which have the option to emit GroupOfFrames instead of separate Frames. 
 * 
 * @author Corne Versloot
 *
 */
public class GroupOfFramesOp implements ISingleInputOperation<CVParticle>{

	private static final long serialVersionUID = 2908327532931814724L;
	private IBatchOperation<CVParticle> operation;
	
	public GroupOfFramesOp(IBatchOperation<CVParticle> operationToExecute){
		this.operation = operationToExecute;
	}

	@Override
	public void deactivate() {
		operation.deactivate();
	}

	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return operation.getSerializer();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) throws Exception {
		operation.prepare(conf, context);
	}

	@Override
	public List<CVParticle> execute(CVParticle input) throws Exception {
		if(input instanceof GroupOfFrames){
			List<CVParticle> frames = new ArrayList<CVParticle>();
			frames.addAll( ((GroupOfFrames)input).getFrames() );
			return operation.execute( frames);
		}
		return new ArrayList<CVParticle>();
	}

}

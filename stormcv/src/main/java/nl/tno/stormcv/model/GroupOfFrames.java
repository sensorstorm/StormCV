package nl.tno.stormcv.model;

import java.util.List;

import backtype.storm.tuple.Tuple;

/**
 * This {@link CVParticle} implementation represents a group (1 or more) of {@link Frame} objects and can be used
 * to send multiple frames at once to a Bolt.
 * 
 * @author Corne Versloot
 *
 */
public class GroupOfFrames extends CVParticle{

	private List<Frame> frames;
	
	public GroupOfFrames(String streamId, long sequenceNr, List<Frame> frames) {
		super(streamId, sequenceNr);
		this.frames = frames;
	}
	
	public GroupOfFrames(Tuple tuple, List<Frame> list) {
		super(tuple);
		this.frames = list;
	}
	
	public List<Frame> getFrames(){
		return frames;
	}
	
	public int nrOfFrames(){
		return frames.size();
	}

}

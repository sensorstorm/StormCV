package nl.tno.stormcv.model;

import nl.tno.stormcv.operation.FramesToVideoOp;
import nl.tno.stormcv.operation.VideoToFramesOp;
import backtype.storm.tuple.Tuple;

/**
 * Represents a piece of video containing a number of encoded frames. The {@link FramesToVideoOp} and {@link VideoToFramesOp} operations can be
 * used to convert a bunch of {@link Frame}'s into a {@link VideoChunk} and the other way around. Videochunks can be used to minimize bandwidth usage
 * compared to sending raw frames. Encoding and decoding of the video however uses more computational power. 
 *  
 * @author Corne Versloot
 *
 */
public class VideoChunk extends CVParticle {

	private long numberOfFrames;
	private byte[] video;
	private String container;

	public VideoChunk(String streamId, long sequenceNr, long nrOfFrames, byte[] videoBytes, String container) {
		super(streamId, sequenceNr);
		this.numberOfFrames = nrOfFrames;
		this.video = videoBytes;
		this.container = container;
	}
	
	public VideoChunk(Tuple tuple, long duration, byte[] videoBlob, String container){
		super(tuple);
		this.numberOfFrames = duration;
		this.video = videoBlob;
		this.container = container;
	}
	
	public long getDuration() {
		return numberOfFrames;
	}
	
	public byte[] getVideo() {
		return video;
	}

	public String getContainer() {
		return container;
	}
	
}

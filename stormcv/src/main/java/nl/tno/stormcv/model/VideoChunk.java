package nl.tno.stormcv.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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

	public VideoChunk(String streamId, long sequenceNr, long duration, byte[] videoBytes) {
		super(streamId, sequenceNr);
		this.numberOfFrames = duration;
		this.video = videoBytes;
	}
	
	@SuppressWarnings("resource")
	public VideoChunk(String streamId, long sequenceNr, long duration, File videoFile) throws IOException {
		super(streamId, sequenceNr);
		this.numberOfFrames = duration;
		FileChannel channel = new FileInputStream(videoFile).getChannel();
		MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
		channel.close();
		this.video = buffer.array();
	}


	public VideoChunk(Tuple tuple, long duration, byte[] videoBlob){
		super(tuple);
		this.numberOfFrames = duration;
		this.video = videoBlob;
	}
	
	public long getDuration() {
		return numberOfFrames;
	}
	
	public byte[] getVideo() {
		return video;
	}
}

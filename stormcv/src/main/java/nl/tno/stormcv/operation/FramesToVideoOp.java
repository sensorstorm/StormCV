package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.TopologyContext;

import com.xuggle.xuggler.ICodec;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.VideoChunk;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.model.serializer.VideoChunkSerializer;
import nl.tno.stormcv.operation.IBatchOperation;
import nl.tno.stormcv.util.StreamWriter;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;

/**
 * A BatchOperation that creates video files or {@link VideoChunk} objects out of the frames it receives. The framesPerVideo parameter defines how big the video's will be.
 * 
 * This operation has two modes; writes {@link Frame}'s to a set of video files with name 'streamId_index.mp4' using h264 encoding. The index starts at 0 and is increased with 1 for
 * <ul>
 * <li>output {@link VideoChunk} objects: the video's generated will be send to the next bolt in the topology. Video will be encoded using the specified codec and put in
 * a wmv container. Video objects can be converted back into either {@link Frame} or {@link GroupOfFrames} using the {@link VideoToFramesOp}. This used less bandwidth than sending
 * the frames individually (raw). The actual bytesize of video chunks can be minimized by setting a high speed (also depends on framerate)</li>
 * <li>write video files to the specified (remote) location and sent the original Frames it gets to the next bolt in the topology. 
 * Each file has an index value, starting at 0, which is incremented for each file written. </li>
 * </ul>
 * The number of frames that have to be written to each individual video chunk must be specified upon construction of this class.
 * By default the video's have normal speed but this can be changed by specifying a speed factor.
 * 
 * Example: a stream is read by a spout and 1 frame every second is emitted into the topology. A StreamWriterOp configured to put 60 frames in a 
 * file and a speed factor of 2.0 will create video's of 30 seconds long (running at twice the normal speed).
 * 
 * It is possible to specify additional parameters like bitrate and ffmpeg parameters to tune the speed and quality.
 * 
 * @author Corne Versloot
 *
 */
public class FramesToVideoOp implements IBatchOperation<CVParticle> {

	private static final long serialVersionUID = 5155420608757446666L;
	private String location;
	private String container;
	private ConcurrentHashMap<String, StreamWriter> writers;
	private float speed = 1;
	private ConnectorHolder connectorHolder;
	private long framesPerVideo;
	private int bitrate = -1;
	private String[] ffmpegParams;
	private ICodec.ID codec = ICodec.ID.CODEC_ID_H264;
	
	/**
	 * Constructs a writer that will put files in the provided location (must be a directory!). Each video
	 * will contain at last framesPerFile number of frames.
	 * @param location the directory where video's must be written to (can be a remote location like ftp if there is a {@link FileConnector} present)
	 * @param framesPerVideo minimum number of frames put in a single file
	 */
	public FramesToVideoOp(String location, long framesPerVideo){
		this.location = location;
		this.framesPerVideo = framesPerVideo;
		this.container = "mp4";
	}
	
	/**
	 * Constructs a writer that will generate {@link VideoChunk} objects from frames it receives. Each video
	 * will contain at last framesPerFile number of frames. By default the WMV2 codec within a wmv container is used
	 * because this combination avoids framelos due to Xuggler/ffmpeg holding frames in buffers.  
	 *   
	 * @param framesPerFile minimum number of frames put in a single file
	 */
	public FramesToVideoOp(long framesPerFile){
		this.location = null;
		this.framesPerVideo = framesPerFile;
		this.container = "wmv";
		this.codec = ICodec.ID.CODEC_ID_WMV2;
	}

	/**
	 * Specifies the speed factor of video's being written, default = 1. A speed of > 1 will increase the speed, < 1 will 
	 * decrease the speed.
	 * @param speed
	 * @return
	 */
	public FramesToVideoOp speed(float speed){
		this.speed = speed;
		return this;
	}
	
	/**
	 * Specifies the container used. Default is mp4
	 * @param ext
	 * @return
	 */
	public FramesToVideoOp container(String ext){
		this.container = ext;
		return this;
	}
	
	/**
	 * Sets the bitrate to be used (number of bytes per second)
	 * @param value
	 * @return itself
	 */
	public FramesToVideoOp bitrate(int value){
		this.bitrate = value;
		return this;
	}
	
	/**
	 * Sets the code to be used
	 * @param codec
	 * @return
	 */
	public FramesToVideoOp codec (ICodec.ID codec){
		this.codec = codec;
		return this;
	}
	
	/**
	 * Specify any ffmpeg flags and their values as a single list. The leading dash typically typed
	 * before a flag should be omitted. Example params: "threads", "2", "tune", "zerolatency". 
	 * @param params
	 * @return itself
	 */
	public FramesToVideoOp ffmpegParams(String... params){
		this.ffmpegParams = params;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(stormConf);
		writers = new ConcurrentHashMap<String, StreamWriter>();
	}

	@Override
	public void deactivate() {
		for(String streamId : writers.keySet()){
			writers.get(streamId).close();
		}
		writers.clear();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public CVParticleSerializer getSerializer() {
		if(location == null) return new VideoChunkSerializer();
		else return new FrameSerializer();
	}

	@Override
	public List<CVParticle> execute(List<CVParticle> input) throws Exception {
		List<CVParticle> result =  new ArrayList<CVParticle>();
		if(input == null || input.size() == 0) return result;
		
		String streamId = input.get(0).getStreamId();
		if(!writers.containsKey(streamId)){
			if(location != null){
				FileConnector fl = connectorHolder.getConnector(location);
				if(fl != null){
					fl = fl.deepCopy();
					fl.moveTo(location);
					writers.put(streamId, new StreamWriter(location, container, fl, this.codec , speed, framesPerVideo, bitrate, ffmpegParams));
				}
			}else{
				writers.put(streamId, new StreamWriter(this.codec, speed, framesPerVideo, container, bitrate, ffmpegParams));
			}
		}
		
		List<Frame> frames = new ArrayList<Frame>(input.size());
		for(CVParticle particle : input) frames.add((Frame)particle);
		
		if(location == null){
			byte[] bytes = writers.get(streamId).addFrames(frames);
			if(bytes != null){
				VideoChunk video = new VideoChunk(streamId, input.get(0).getSequenceNr(), framesPerVideo, bytes, container);
				result.add(video);
			}
			return result;
		}else{
			writers.get(streamId).addFrames(frames);
			return input;
		}
	}

}

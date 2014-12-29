package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.TopologyContext;

import com.xuggle.xuggler.ICodec;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.IBatchOperation;
import nl.tno.stormcv.util.StreamWriter;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;

/**
 * A BatchOperation that writes {@link Frame}'s to a set of video files with name 'streamId_index.mp4' using h264 encoding. The index starts at 0 and is increased with 1 for
 * each new file being created. The number of frames that have to be written to each individual file must be specified upon construction of this class.
 * If a file contains this number of frames it is closed and a new file with incremented index is created. Files are written in the local tmp directory 
 * and then moved to the specified remote location. By default the video's have normal speed but this can be changed by specifying a speed factor.
 * 
 * Example: a stream is read by a spout and 1 frame every second is emitted into the topology. A StreamWriterOp configured to put 60 frames in a 
 * file and a speed factor of 2.0 will create video files of 30 seconds long (running at twice the normal speed).
 * 
 * 
 * @author Corne Versloot
 *
 */
public class VideoFileWriterOp implements IBatchOperation<Frame> {

	private static final long serialVersionUID = 5155420608757446666L;
	private FrameSerializer serializer = new FrameSerializer();
	private String location;
	private ConcurrentHashMap<String, StreamWriter> writers;
	private float speed = 1;
	private ConnectorHolder connectorHolder;
	private long framesPerFile;
	
	/**
	 * Constructs a writer that will put files in the provided location (must be a directory!). Each video
	 * will contain at last framesPerFile number of frames.
	 * @param location the directory where video's must be written to (can be a remote location like ftp if there is a {@link FileConnector} present)
	 * @param framesPerFile minimum number of frames put in a single file
	 */
	public VideoFileWriterOp(String location, long framesPerFile){
		this.location = location;
		this.framesPerFile = framesPerFile;
	}

	/**
	 * Specifies the speed factor of video's being written, default = 1. A speed of > 1 will increase the speed, < 1 will 
	 * decrease the speed.
	 * @param speed
	 * @return
	 */
	public VideoFileWriterOp speed(float speed){
		this.speed = speed;
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

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}

	@Override
	public List<Frame> execute(List<CVParticle> input) throws Exception {
		if(input == null || input.size() == 0) return new ArrayList<Frame>();
		
		String streamId = input.get(0).getStreamId();
		if(!writers.containsKey(streamId)){
			FileConnector fl = connectorHolder.getConnector(location);
			if(fl != null){
				fl = fl.deepCopy();
				fl.moveTo(location);
				writers.put(streamId, new StreamWriter(location, fl, ICodec.ID.CODEC_ID_H264, speed, framesPerFile));
			}
		}
		List<Frame> frames = new ArrayList<Frame>(input.size());
		for(CVParticle particle : input) frames.add((Frame)particle);
		writers.get(streamId).addFrames(frames);
		return frames;
	}

}

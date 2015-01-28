package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.VideoChunk;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.model.serializer.GroupOfFramesSerializer;
import nl.tno.stormcv.util.ImageUtils;

/**
 * Converts received {@link VideoChunk} objects back to {@link Frame}'s (optionally wrapped in {@link GroupOfFrames}).
 * Using video chunks consumes less bandwidth than raw frames.
 * 
 * @author Corne Versloot
 *
 */
public class VideoToFramesOp extends MediaListenerAdapter implements ISingleInputOperation<CVParticle> {

	private static final long serialVersionUID = -5760365349534351251L;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
			
	private int frameSkip;
	private boolean groupOfFrames = false;
	private List<Frame> frames;
	private long seqNum;
	private String streamId;
	private String imageType = Frame.JPG_IMAGE;

	/**
	 * Instantiate a VideoToFrames operation by specifying the frameskip used to create the video (needed to calculate the correct
	 * frameNumbers for the frames in the video. By default the frames in the video will be emitted to the next bolt individually.
	 * This behavior can be changed using the groupFrames setting.  
	 * @param frameSkip
	 */
	public VideoToFramesOp(int frameSkip, boolean groupOfFrames){
		this.frameSkip = frameSkip;
		this.groupOfFrames = groupOfFrames;
	}
	
	/**
	 * Indicates that the frames from the video must be emitted as a {@link GroupOfFrames} object rather than individual 
	 * frames.
	 * @return itself
	 */
	public VideoToFramesOp groupFrames(){
		groupOfFrames = true;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context)	throws Exception {
		if(conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)){
			imageType  = (String)conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
		}
	}

	@Override
	public void deactivate() {	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public CVParticleSerializer getSerializer() {
		if(groupOfFrames) return new GroupOfFramesSerializer();
		else return new FrameSerializer();
	}

	@Override
	public List<CVParticle> execute(CVParticle particle) throws Exception {
		List<CVParticle> result = new ArrayList<CVParticle>();
		if(!(particle instanceof VideoChunk)) return result;
		
		VideoChunk video = (VideoChunk) particle;
		frames = new ArrayList<>();
		seqNum = video.getSequenceNr();
		streamId = video.getStreamId();
		
		IContainerFormat format = IContainerFormat.make();
		format.setInputFormat("flv");
		
		IContainer container = IContainer.make();
		ByteArrayInputStream bais = new ByteArrayInputStream(video.getVideo());

		container.setInputBufferLength(bais.available());
		container.open(bais, format, false, false);
		IMediaReader reader = ToolFactory.makeReader(container);
		
		reader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
		reader.addListener(this);
		while (reader.readPacket() == null);
		reader.close();
		
		if(groupOfFrames){
			result.add(new GroupOfFrames(video.getStreamId(), video.getSequenceNr(), frames));
		}else{
			for(Frame frame : frames) result.add(frame);
		}
		System.err.println(result.size());
		return result;
	}
	
	public void onVideoPicture(IVideoPictureEvent event) {
		try{
			BufferedImage frame = event.getImage();
			byte[] buffer = ImageUtils.imageToBytes(frame, imageType);
			Frame newFrame = new Frame(streamId, seqNum+frames.size()*frameSkip, imageType, buffer, event.getTimeStamp(TimeUnit.MILLISECONDS), new Rectangle(0, 0,frame.getWidth(), frame.getHeight()));
			frames.add(newFrame);
		}catch(IOException ioe){
			logger.error("Exception while decoding video: "+ioe.getMessage(), ioe);
		}
    }

}

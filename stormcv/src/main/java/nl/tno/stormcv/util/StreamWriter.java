package nl.tno.stormcv.util;

import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.util.connector.FileConnector;

import com.xuggle.mediatool.IMediaWriter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.io.XugglerIO;

/**
 * Utility class used to write frames to a video file and used by the StreamWriterOperation to store streams. 
 * By default the video is encoded as H264 at the original speed of the video (stream) being analyzed. This holds even if 
 * the framerate is very low (i.e. the spout only grabs a frame every second or so). It is 
 * possible to provide an additional speed factor (default = 1.0) to speed up or slow down the video being written.
 * The owner of the StreamWriter must call close() to finish the file being written which performs the actual flush and close of the stream. If close is not called the file 
 * will most likely not be playable. 
 * 
 * @author Corne Versloot
 *
 */
public class StreamWriter {

	private Logger logger = LoggerFactory.getLogger(StreamWriter.class);
	private IMediaWriter writer;
	private ICodec.ID codec = ICodec.ID.CODEC_ID_H264;
	private float speed = 1.0f;
	private long tbf;
	private long nextFrameTime;
	private FileConnector connector;
	private String location;
	private File currentFile;;
	private long frameCount;
	private int fileCount;
	private long nrFramesVideo;
	private File tmpDir;
	private ByteArrayOutputStream output;
	
	/**
	 * Creates a StreamWriter which will write provided frames to the location using the provided
	 * speed and codec
	 * @param location the location of the videofile to be written
	 * @param codec the codec to use to encode the video
	 * @param speed the speed to use (1 == normal speed, 2 == twice normal speed and 0.5 == half normal speed)
	 * @param nrFramesVideo indicates how many frames must be written before a video is closed
	 * @throws IOException 
	 */
	public StreamWriter(String location, FileConnector connector, ICodec.ID codec, float speed, long nrFramesVideo) throws IOException{
		this.location = location;
		if(!this.location.endsWith("/")) this.location += "/";
		this.connector = connector;
		this.codec = codec;
		this.speed = speed;
		this.nrFramesVideo = nrFramesVideo;
		tmpDir = File.createTempFile("abc12", "def");
		tmpDir.delete();
		tmpDir = tmpDir.getParentFile();
	}
	
	/**
	 * Creates a StreamWriter which produces bytes as result to addFrames
	 * @param codec
	 * @param speed
	 * @param nrFramesVideo
	 * @throws IOException
	 */
	public StreamWriter(ICodec.ID codec, float speed, long nrFramesVideo) throws IOException{
		this.codec = codec;
		this.speed = speed;
		this.nrFramesVideo = nrFramesVideo;
	}
	
	/**
	 * Creates a mediawriter that will write to a ByteArrayOutputStream instead of a file
	 * This stream will be formatted as mp4.
	 * @throws IOException
	 */
	private void makeBytesWriter() throws IOException{
		if(output != null) output.close();
		output = new ByteArrayOutputStream();
		writer = ToolFactory.makeWriter(XugglerIO.map(output));
		IContainerFormat containerFormat = IContainerFormat.make();
		containerFormat.setOutputFormat("flv", null, null);
		writer.getContainer().setFormat(containerFormat);
	}
	
	/**
	 * Adds the set of frames to the file created by this StreamWriter. The characteristics of the stream
	 * (WxH and frame rate) are inferred automatically from the first set of frames provided which must be 
	 * at least two subsequent frames. 
	 * @param frames the set of frames to be added to the file
	 * @throws IOException 
	 */
	public byte[] addFrames(List<Frame> frames) throws IOException{
		if(writer == null){
			tbf = (long)Math.ceil( (frames.get(frames.size()-1).getTimestamp() - frames.get(0).getTimestamp())/(frames.size()-1));
			tbf = (long)(tbf / speed);
			int width = frames.get(0).getImage().getWidth();
			int height = frames.get(0).getImage().getHeight();
			if(location != null){
				currentFile = new File(tmpDir, frames.get(0).getStreamId()+"_"+fileCount+".mp4");
				logger.info("Writing TMP video to: "+currentFile);
				writer = ToolFactory.makeWriter(currentFile.getAbsolutePath());
			}else{
				makeBytesWriter();
			}
			writer.addVideoStream(0, 0, codec, width, height);
			nextFrameTime = 0;
		}
		
		for(Frame frame : frames){
			BufferedImage image = frame.getImage();
			addFrameToVideo(image);
		}
		if(frameCount >= nrFramesVideo){
			// when writing to memory the last two frames are missing so we add them twice as a (ugly) work around
			// TODO: figure out where this behavior comes from and fix it!
			if(this.output != null){
				addFrameToVideo(frames.get(frames.size()-2).getImage());
				addFrameToVideo(frames.get(frames.size()-1).getImage());
			}
			this.close();
			if(this.output != null) {
				output.flush();
				return output.toByteArray();
			}
		}
		return null;
	}

	private void addFrameToVideo(BufferedImage image){
		// Xuggler requires 3byte rgb encoded images so check and convert if needed
		if(image.getType() != BufferedImage.TYPE_3BYTE_BGR){
			BufferedImage imageBGR = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
			new ColorConvertOp(null).filter(image, imageBGR);
			image = imageBGR;
		}
		writer.encodeVideo(0, image, nextFrameTime, TimeUnit.MILLISECONDS);
		nextFrameTime += tbf;
		frameCount++;
	}
	
	/**
	 * Closes the video file and makes it playable. This method must be called by the owner or
	 * the file will never be closed and probably not playable
	 */
	public void close(){
		if(writer != null ) {
			frameCount = 0;
			fileCount++;
			writer.flush();
			writer.close();
			if(location != null) try {
				connector.moveTo(location+currentFile.getName());
				logger.info("Moving video to final location: "+location+currentFile.getName());
				connector.copyFile(currentFile, true);
			} catch (IOException e) {
				logger.error("Unable to move file to final destinaiton: location", e);
			}
			writer = null;
		}
	}
	
}

package nl.tno.stormcv.util;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.util.connector.FileConnector;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IMetaData;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IRational;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.IPixelFormat.Type;
import com.xuggle.xuggler.video.ConverterFactory;
import com.xuggle.xuggler.video.IConverter;
import com.xuggle.xuggler.IStreamCoder;

/**
 * Utility class used to write frames to a video file and used by the StreamWriterOperation to store streams. 
 * By default the video is encoded as H264 at the original speed of the video (stream) being analyzed. This holds even if 
 * the framerate is very low (i.e. the spout only grabs a frame every second or so). It is 
 * possible to provide an additional speed factor (default = 1.0) to speed up or slow down the video being written.
 * 
 * @author Corne Versloot
 *
 */
public class StreamWriter {

	private Logger logger = LoggerFactory.getLogger(StreamWriter.class);
	private ICodec.ID codec = ICodec.ID.CODEC_ID_H264;
	private float speed = 1.0f;
	private FileConnector connector;
	private String location;
	private String container;
	private File currentFile;
	private long frameCount;
	private int fileCount;
	private long nrFramesVideo;
	private File tmpDir;
	
	private IStreamCoder coder;
	private int bitrate = -1;
	private int frameTime;
	private IContainer writer;
	private int frameRate;
	private String[] ffmpegParams;
	
	/**
	 * Creates a StreamWriter which will write provided frames to the location using the provided
	 * speed and codec
	 * @param location the location of the videofile to be written
	 * @param codec the codec to use to encode the video
	 * @param speed the speed to use (1 == normal speed, 2 == twice normal speed and 0.5 == half normal speed)
	 * @param nrFramesVideo indicates how many frames must be written before a video is closed
	 * @throws IOException 
	 */
	public StreamWriter(String location, String extension, FileConnector connector, ICodec.ID codec, float speed, long nrFramesVideo, int bitrate, String... flags) throws IOException{
		this(codec, speed, nrFramesVideo, extension, bitrate, flags);
		this.location = location;
		if(!this.location.endsWith("/")) this.location += "/";
		this.connector = connector;
	}
	
	/**
	 * Creates a StreamWriter which produces bytes as result to addFrames
	 * @param codec
	 * @param speed
	 * @param nrFramesVideo
	 * @throws IOException
	 */
	public StreamWriter(ICodec.ID codec, float speed, long nrFramesVideo, String container, int bitrate, String... flags) throws IOException{
		this.codec = codec;
		this.speed = speed;
		this.bitrate = bitrate;
		this.ffmpegParams = flags;
		this.nrFramesVideo = nrFramesVideo;
		this.container = (container.startsWith(".") ? container.substring(1) : container);
		this.codec = codec;
		this.speed = speed;
		this.nrFramesVideo = nrFramesVideo;
		tmpDir = File.createTempFile("abc12", "def");
		tmpDir.delete();
		tmpDir = tmpDir.getParentFile();
	}
	
	/**
	 * Make a video writer for the given path and dimensions
	 * @param path
	 * @param width
	 * @param height
	 */
	private void makeWriter(String path, int width, int height){
		writer = IContainer.make();
		writer.open(path, IContainer.Type.WRITE, null);
		
		ICodec videoCodec = ICodec.findEncodingCodec(codec);
		IStream videoStream = writer.addNewStream(videoCodec);
		coder = videoStream.getStreamCoder();
		
		IRational fr = IRational.make(frameRate, 1);
		
		coder.setWidth(width);
		coder.setHeight(height);
		coder.setFrameRate(fr);
		coder.setTimeBase(IRational.make(fr.getDenominator(), fr.getNumerator()));
		coder.setNumPicturesInGroupOfPictures(frameRate);
		if(bitrate > 0) coder.setBitRate(bitrate);
		//coder.setBitRateTolerance(100000);
		coder.setPixelType(Type.YUV420P);
		//coder.setFlag(IStreamCoder.Flags.FLAG2_FAST, true);
		//coder.setGlobalQuality(0);
		
		if(ffmpegParams != null){
			IMetaData options = IMetaData.make();
			for(int i=0; i<ffmpegParams.length; i+=2){
				options.setValue(ffmpegParams[i], ffmpegParams[i+1]);
			}
			coder.open(options, null);
		}else{
			coder.open(null, null);
		}
		writer.writeHeader();
		this.frameTime = 0;
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
			frameRate = (int)(frames.get(1).getTimestamp() - frames.get(0).getTimestamp());
			frameRate = 1000 / frameRate;
			frameRate *= speed;
			BufferedImage frame = frames.get(0).getImage();
			currentFile = new File(tmpDir, frames.get(0).getStreamId()+"_"+fileCount+"."+this.container);
			logger.info("Writing TMP video to: "+currentFile);
			makeWriter(currentFile.getAbsolutePath(), frame.getWidth(), frame.getHeight());
		}
		// add frames to video
		for(Frame frame : frames){
			addImage(frame.getImage());
		}
		
		// check if we have written the required number of frames
		if(frameCount >= nrFramesVideo){
			this.close();
			if(location == null){
				FileInputStream fis = new FileInputStream(currentFile);
				byte[] bytes = new byte[(int)currentFile.length()];
				fis.read(bytes);
				fis.close();
				if(!currentFile.delete()) currentFile.deleteOnExit();
				return bytes;
			}
		}
		return null;
	}
	
	/**
	 * Adds an image as a frame to the current video
	 * @param image
	 */
	private void addImage(BufferedImage image){
		IPacket packet = IPacket.make();
		IConverter converter = ConverterFactory.createConverter(image, coder.getPixelType());
		IVideoPicture frame = converter.toPicture(image, Math.round(frameTime));
		
		if (coder.encodeVideo(packet, frame, 0) < 0) {
			throw new RuntimeException("Unable to encode video.");
		}
		
		if (packet.isComplete()) {
			if (writer.writePacket(packet) < 0) {
				throw new RuntimeException("Could not write packet to container.");
			}
		}
        this.frameTime += 1000000f/frameRate;
        frameCount++;
	}

	public void close(){
		if(writer != null){
			// write last packets
			IPacket packet = IPacket.make();
			do{
				coder.encodeVideo(packet, null, 0);
				if(packet.isComplete()){
					writer.writePacket(packet);
				}
			}while(packet.isComplete());
			writer.flushPackets();
			writer.close();
			writer = null;
			coder = null;
			fileCount++;
			frameCount = 0;
			
			// move video to final location (can be remote!)
			if(location != null) try {
				connector.moveTo(location+currentFile.getName());
				logger.info("Moving video to final location: "+location+currentFile.getName());
				connector.copyFile(currentFile, true);
			} catch (IOException e) {
				logger.error("Unable to move file to final destinaiton: location", e);
			}
		}
	}

	
	/**
	 * Adds the set of frames to the file created by this StreamWriter. The characteristics of the stream
	 * (WxH and frame rate) are inferred automatically from the first set of frames provided which must be 
	 * at least two subsequent frames. 
	 * @param frames the set of frames to be added to the file
	 * @throws IOException 
	 *//*
	public byte[] addFrames(List<Frame> frames) throws IOException{
		if(writer == null){
			start = System.currentTimeMillis();
			tbf = (long)Math.ceil( (frames.get(frames.size()-1).getTimestamp() - frames.get(0).getTimestamp())/(frames.size()-1));
			tbf = (long)(tbf / speed);
			int width = frames.get(0).getImage().getWidth();
			int height = frames.get(0).getImage().getHeight();
			currentFile = new File(tmpDir, frames.get(0).getStreamId()+"_"+fileCount+"."+this.container);
			logger.info("Writing TMP video to: "+currentFile);
			writer = ToolFactory.makeWriter(currentFile.getAbsolutePath());
			writer.addVideoStream  (0, 0, codec, IRational.make(1000f/tbf), width, height);
			nextFrameTime = 0;
		}
		
		for(Frame frame : frames){
			BufferedImage image = frame.getImage();
			addFrameToVideo(image);
		}
		if(frameCount >= nrFramesVideo){
			System.err.println(frameCount+" in "+(System.currentTimeMillis() - start)/1000+" sec");
			this.close();
			if(location == null){
				FileInputStream fis = new FileInputStream(currentFile);
				byte[] bytes = new byte[(int)currentFile.length()];
				fis.read(bytes);
				fis.close();
				if(!currentFile.delete()) currentFile.deleteOnExit();
				return bytes;
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
	*/
	/**
	 * Closes the video file and makes it playable. This method must be called by the owner or
	 * the file will never be closed and probably not playable
	 */
	/*
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
	*/
	
}

package nl.tno.stormcv.fetcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.GroupOfFramesSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.GroupOfFramesOp;
import nl.tno.stormcv.util.StreamReader;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

/**
 * FileFrameFetcher is responsible for extracting frames from video files. These files can reside on any location as long
 * as a {@link FileConnector} was registered for it at the {@link StormCVConfig}. StromCV has some connectors that can be used
 * out of the box: local files and AWS S3. It is possible to add other connectors by implementing the {@link FileConnector} interface.
 * 
 * If FileFrameFetcher is initialized with a directory instead of a file the directory will be expanded and all files with video extensions 
 * will be listed instead (recursively!). The expanded list will be divided among all FileFrameFetchers operating in the
 * topology. Each FileFrameFetcher will process its own list sequentially until it is done. Each file is downloaded to the 
 * local tmp directory of the Spout executing the fetcher and deleted afterwards.
 * 
 * The frameSkip and groupSize parameters define which frames will be extracted using: frameNr % frameSkip < groupSize-1
 * With a frameSkip of 10 and groupSize of 2 the following framenumbers will be extracted: 0,1,10,11,20,21,.. Both frameskip and
 * groupSize have default value 1 which means that all frames are read.
 * 
 * By default all files will get a unique streamId. If idPerFile is set to false all files will get the same streamId and 
 * frame numbering is continued as if the files form a single stream. This can be the case when reading a directory with
 * HLS segments. 
 * 
 * This fetcher can be configured to emit {@link GroupOfFrames} objects instead of {@link Frame} by using the groupOfFramesOutput method. Emitting
 * a {@link GroupOfFrames} can be useful when the subsequent Operation requires multiple frames of the same stream in which case the
 * {@link GroupOfFramesOp} can be used without a batcher. 
 *   
 * @author Corne Versloot
 *
 */
public class FileFrameFetcher implements IFetcher<CVParticle> {

	private static final long serialVersionUID = 2851573165386120721L;
	private Logger logger = LoggerFactory.getLogger(FileFrameFetcher.class);
	private List<Frame> frameGroup;
	private int frameSkip = 1;
	private int groupSize = 1;
	private List<String> locations;
	private StreamReader streamReader;
	private LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(100);
	private int sleepTime = 0;
	private ConnectorHolder connectorHolder;
	private boolean useSingleId = false;
	private int batchSize = 1;
	private String imageType;
	
	/**
	 * Sets the locations this fetcher will read video from. The list is split evenly
	 * between all fetchers active in the topology
	 * @param locations
	 * @return
	 */
	public FileFrameFetcher (List<String> locations){
		this.locations = locations;
	}
	
	/**
	 * Sets the number of frames to skip before extracting a frame 
	 * @param skip
	 * @return
	 */
	public FileFrameFetcher frameSkip(int skip){
		this.frameSkip = skip;
		return this;
	}
	
	/**
	 * Sets the number of frames for a group
	 * @param size
	 * @return
	 */
	public FileFrameFetcher groupSize(int size){
		this.groupSize = size;
		return this;
	}
	
	/**
	 * @param 
	 * @return
	 */
	public FileFrameFetcher sleep (int ms){
		this.sleepTime = ms;
		return this;
	}
	/**
	 * Specify if all files read by this fetcher must get the same streamId (default is false). If set to true all files read will get
	 * the same streamId and frame numbering is continued (as if the files form a continuous stream)
	 * @param value
	 * @return
	 */
	public FileFrameFetcher singleId (boolean value){
		this.useSingleId  = value;
		return this;
	}
	
	/**
	 * Specifies the number of frames to be send at once. If set to 1 (default value) this Fetcher will emit
	 * {@link Frame} objects. If set to 2 or more it will emit {@link GroupOfFrames} objects.
	 * @param nrFrames
	 * @return
	 */
	public FileFrameFetcher groupOfFramesOutput(int nrFrames){
		this.batchSize = nrFrames;
		return this;
	}
	
	@SuppressWarnings({ "rawtypes"})
	@Override
	public void prepare(Map conf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(conf);

		if(conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)){
			imageType = (String)conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
		}
		
		List<String> original = new ArrayList<String>();
		original.addAll(locations);
		locations.clear();
		for(String dir : original){
			locations.addAll(expand(dir));
		}
		
		int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		// change the list based on the number of tasks working on it
		if (this.locations != null && this.locations.size() > 0) {
			int batchSize = (int) Math.floor(locations.size() / nrTasks) + 1;
			int start = batchSize * taskIndex;
			locations = locations.subList(start, Math.min(start + batchSize, locations.size()));
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public CVParticleSerializer getSerializer() {
		if(batchSize <= 1) return new FrameSerializer();
		else return new GroupOfFramesSerializer();
	}
	
	@Override
	public void activate() {
		if(streamReader != null){
			this.deactivate();
		}
		
		LinkedBlockingQueue<String> videoList = new LinkedBlockingQueue<String>(10);
		DownloadThread dt = new DownloadThread(locations, videoList, connectorHolder);
		new Thread(dt).start();
		
		streamReader = new StreamReader(videoList, imageType, frameSkip, groupSize, sleepTime, useSingleId,  frameQueue);
		new Thread(streamReader).start();
	}
	
	@Override
	public void deactivate() {
		if(streamReader != null) streamReader.stop();
		streamReader = null;
	}

	@Override
	public CVParticle fetchData() {
		if(streamReader == null) this.activate();
		Frame frame = frameQueue.poll();
		if(frame != null) {
			if(batchSize <= 1){
				return frame;
			}else{
				if(frameGroup == null || frameGroup.size() >= batchSize) frameGroup = new ArrayList<Frame>();
				frameGroup.add(frame);
				if(frameGroup.size() == batchSize){
					return new GroupOfFrames(frameGroup.get(0).getStreamId(), frameGroup.get(0).getSequenceNr(), frameGroup);
				}
			}
		}
		
		if(streamReader == null || !streamReader.isRunning()){
			if(streamReader != null){
				streamReader.stop();
				streamReader = null;
			}
			this.activate();
		}
		return null;
	}
	
	/**
	 * Lists all files in the specified location. If the location itself is a file the location will be the only
	 * object in the result. If the location is a directory (or AWS S3 prefix) the result will contain all files
	 * in the directory. Only files with correct extensions will be listed!
	 * @param location
	 * @return
	 */
	private List<String> expand(String location){
		FileConnector fl = connectorHolder.getConnector(location);
		if(fl != null){
			fl.setExtensions(new String[]{".m2v", ".mp4", ".mkv", ".ts", ".flv", ".avi", ".mov", ".wmv", ".mpg", ".mpeg"});
			try {
				fl.moveTo(location);
			} catch (IOException e) {
				logger.warn("Unable to move to "+location+" due to: "+e.getMessage());
				return new ArrayList<String>();
			}
			return fl.list();
		}else return new ArrayList<String>();
	}

	private class DownloadThread implements Runnable{

		private List<String> locations;
		private ConnectorHolder connectorHolder;
		private LinkedBlockingQueue<String> videoList;
		
		public DownloadThread(List<String> locations, LinkedBlockingQueue<String> videoList, ConnectorHolder connectorHolder){
			this.locations = locations;
			this.videoList = videoList;
			this.connectorHolder = connectorHolder;
		}
		
		@Override
		public void run() {
			while(locations.size() > 0) try{
				String location = locations.remove(0);
				FileConnector connector = connectorHolder.getConnector(location);
				if(connector != null){
					connector.moveTo(location);
					File localFile = connector.getAsFile();
					if(!(connector instanceof LocalFileConnector))localFile.deleteOnExit();
					videoList.put(localFile.getAbsolutePath());
				}
			}catch(Exception e){
				logger.warn("Unable to download file due to: "+e.getMessage(), e);
			}
		}
		
	}
	
}



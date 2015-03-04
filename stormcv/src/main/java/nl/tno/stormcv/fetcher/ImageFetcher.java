package nl.tno.stormcv.fetcher;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

/**
 * A {@link IFetcher} implementation that reads images and emits them into the topology as {@link Frame} objects. The ImageFetcher
 * works in the same way as the {@link FileFrameFetcher}; it expands locations it gets looking for image files, spreads all images
 * among all ImageFetcher instances within the topology and starts processing them. 
 * 
 * @author Corne Versloot
 *
 */

public class ImageFetcher implements IFetcher<Frame> {

	private static final long serialVersionUID = -7649344867160068896L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private FrameSerializer serializer = new FrameSerializer();
	private List<String> locations;
	private int sleepTime = 0;
	private ConnectorHolder connectorHolder;
	private String imageType = Frame.JPG_IMAGE;

	/**
	 * Sets the locations this fetcher will read images from. The list is split evenly
	 * between all fetchers active in the topology
	 * @param locations
	 */
	public ImageFetcher(List<String> locations){
		this.locations = locations;
	}
	
	/**
	 * Sets the time to sleep after each image read. The fetcher can be throttled
	 * in this way.
	 * @param sleep
	 * @return
	 */
	public ImageFetcher sleepTime(int sleep){
		this.sleepTime = sleep;
		return this;
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context)	throws Exception {
		this.connectorHolder = new ConnectorHolder(stormConf);
		if(stormConf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)){
			imageType = (String)stormConf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
		}
		
		int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
		
		List<String> original = new ArrayList<String>();
		original.addAll(locations);
		locations.clear();
		for(String dir : original){
			locations.addAll(expand(dir));
		}
		
		// change the list based on the number of tasks working on it
		List<String> filesToFetch = new ArrayList<String>();
		int i = context.getThisTaskIndex();
		while(i < locations.size()){
			filesToFetch.add(locations.get(i));
			i += nrTasks;
		}
		this.locations = filesToFetch;
		
	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
	}

	@Override
	public Frame fetchData() {
		Frame frame = null;
		if(locations.size() > 0){
			String imgFile = locations.remove(0);
			if(imgFile.startsWith("http://")) try{
				BufferedImage image = ImageIO.read(new URL(imgFile));
				byte[] buffer = ImageUtils.imageToBytes(image, imageType);
				frame = new Frame(imgFile.substring(imgFile.lastIndexOf('/'))+"_"+imgFile.hashCode(), 0, imageType, buffer, 0, new Rectangle(0, 0, image.getWidth(), image.getHeight()));
				frame.getMetadata().put("uri", imgFile);
			} catch(Exception e){
				logger.warn("Unable to get image from "+imgFile+" due to : "+e.getMessage(), e);	
			}
			else try{
				FileConnector fl = connectorHolder.getConnector(imgFile);
				if(fl != null){
					fl.moveTo(imgFile);
					File file = fl.getAsFile();
					try{
						BufferedImage image = ImageIO.read(file);
						byte[] buffer = ImageUtils.imageToBytes(image, imageType);
						frame = new Frame(file.getName()+"_"+file.hashCode(), 0, imageType, buffer, 0, new Rectangle(0, 0, image.getWidth(), image.getHeight()));
						frame.getMetadata().put("uri", imgFile);
						if(!(fl instanceof LocalFileConnector)){
							file.delete();
						}
					}catch(Exception e){
						if(!(fl instanceof LocalFileConnector)){
							file.delete();
						}
						throw e;
					}
				}
			}catch(Exception e){
				logger.warn("Unable to read image from "+imgFile+" due to "+e.getMessage());
			}
		}
		Utils.sleep(sleepTime);
		return frame;
	}

	/**
	 * Lists all files in the specified location. If the location itself is a file the location will be the only
	 * object in the result. If the location is a directory (or AQS S3 prefix) the result will contain all files
	 * in the directory. Only files with correct extensions will be listed!
	 * @param location
	 * @return
	 */
	private List<String> expand(String location){
		FileConnector fl = connectorHolder.getConnector(location);
		
		if(fl != null){
			fl.setExtensions(new String[]{".jpg", ".JPG", ".jpeg", ".JPEG", ".png", ".PNG", ".gif", ".GIF", ".bmp", ".BMP"});
			try {
				fl.moveTo(location);
			} catch (IOException e) {
				logger.warn("Unable to move to "+location+" due to: "+e.getMessage());
				return new ArrayList<String>();
			}
			return fl.list();
		}else return new ArrayList<String>();
	}
}

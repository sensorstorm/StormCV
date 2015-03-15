package nl.tno.stormcv.example.drpc;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.OpenCVOp;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfDMatch;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DMatch;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.DescriptorMatcher;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;

/**
 * A simple operation that reads part of the prototype set upon prepare. What prototypes to read depends
 * on the number operations present within the topology. The input provided with the 
 * execute method is compared with the prototypes stored during prepare. Any prototype that matches
 * at least miNStrongMatches is added to the result which is put in the metadata map of the input (with
 * key = strong_matches).
 * 
 * @author Corne Versloot
 *
 */
public class PartialMatcher extends OpenCVOp<Frame> implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = -7759756124507057013L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private static final String[] ext = new String[]{".jpg", ".JPG", ".jpeg", ".JPEG", ".png", ".PNG", ".gif", ".GIF", ".bmp", ".BMP"};
	private ConnectorHolder connectorHolder;
	private int detectorType = FeatureDetector.SIFT;
	private int descriptorType = DescriptorExtractor.SIFT;
	private int matcherType = DescriptorMatcher.BRUTEFORCE;
	private int minStrongMatches = 3;
	private float minStrongMatchDist = 0.5f;

	private DescriptorMatcher matcher;
	private HashMap<Mat, String> prototypes;
	private List<String> protoLocations;

	/**
	 * Creates a PartialMatcher using the prototype images located in the provided list. Each URI
	 * in the list is expanded to search for more prototypes (i.e. it is possible to provide a single directory) 
	 * @param prototypes list with URI's pointing to prototype images to be loaded
	 * @param minStrongMatches minimum number of strong matching keypoints
	 * @param minStrongMatchDist
	 */
	public PartialMatcher(List<String> prototypes, int minStrongMatches, float minStrongMatchDist){
		this.protoLocations = prototypes;
		this.minStrongMatches = minStrongMatches;
		this.minStrongMatchDist = minStrongMatchDist;
	}
	
	/**
	 * set the feature detector to use (default = FeatureDetector.SIFT)
	 * @param type
	 * @return
	 */
	public PartialMatcher detector(int type){
		this.detectorType = type;
		return this;
	}
	
	/**
	 * set the feature descriptor to use (default = DescriptorExtractor.SIFT)
	 * @param type
	 * @return
	 */
	public PartialMatcher descriptor(int type){
		this.descriptorType = type;
		return this;
	}
	
	/**
	 * set the matching method to use (default = DescriptorMatcher.BRUTEFORCE)
	 * @param type
	 * @return
	 */
	public PartialMatcher matcher(int type){
		this.matcherType = type;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map conf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(conf);
		matcher = DescriptorMatcher.create( matcherType );
		prototypes = new HashMap<Mat, String>();
		
		int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		List<String> original = new ArrayList<String>();
		original.addAll(protoLocations);
		protoLocations.clear();
		for(String dir : original){
			protoLocations.addAll(expand(dir));
		}
		
		// read each prototype and add it to the list 
		FileConnector fc = null;
		for(int i=taskIndex; i<protoLocations.size(); i+=nrTasks){
			String imgFile = protoLocations.get(i);
			fc = connectorHolder.getConnector(imgFile);
			fc.moveTo(imgFile);
			File imageFile = fc.getAsFile();
			BufferedImage img = ImageIO.read(imageFile);
			if(img == null) continue;
			Mat proto = calculateDescriptors(img);
			prototypes.put(proto, imgFile.substring(imgFile.lastIndexOf('/')+1));
			logger.info(this.getClass().getName()+"["+taskIndex+"] "+imgFile+" loaded and prepared for matching");
			if(!(fc instanceof LocalFileConnector)) imageFile.delete();
		}
	}

	@Override
	public void deactivate() {
		prototypes.clear();
	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		Frame frame = null;
		Mat frameDescriptor = null;
		if(particle instanceof Frame) {
			frame = (Frame)particle;
			if(frame.getImageType().equals(Frame.NO_IMAGE)) return result;
			frameDescriptor = this.calculateDescriptors(frame.getImageBytes());
		}else if(particle instanceof Feature){
			Feature feature = (Feature)particle;
			frameDescriptor = this.feature2Mat(feature);
			frame = new Frame(feature.getStreamId(), feature.getSequenceNr(), Frame.NO_IMAGE, new byte[]{}, 0, new Rectangle(0,0,0,0));
		}else return result;
		
		result.add(frame);
		
		String matchStr = new String();
		for(Mat proto : prototypes.keySet()){
			int strongMatches = 0;
			List<MatOfDMatch> matches = new ArrayList<MatOfDMatch>();
			matcher.knnMatch( frameDescriptor, proto, matches, 2 );
			for ( int i = 0; i < matches.size(); i++ ){
				DMatch dmatches[] = matches.get(i).toArray();
				if ( dmatches[0].distance < minStrongMatchDist * dmatches[1].distance ){
					strongMatches++;
				}
			}
			
			if(strongMatches >= minStrongMatches){
				matchStr += "\""+prototypes.get(proto)+"\":"+strongMatches+";";
			}
		}
		frame.getMetadata().put("strong_matches", matchStr);
		return result;
	}

	/**
	 * Calculates descriptors as defined by detectorType and 
	 * descriptorType provided at construction for the provided image
	 * @param input
	 * @return
	 * @throws IOException
	 */
	private Mat calculateDescriptors(BufferedImage input) throws IOException{
		byte[] buffer;
		buffer = ImageUtils.imageToBytes(input, "png");
		return calculateDescriptors(buffer);
	}
	
	/**
	 * Calculates descriptors as defined by detectorType and 
	 * descriptorType provided at construction for the provided image
	 * @param input
	 * @return
	 * @throws IOException
	 */
	private Mat calculateDescriptors(byte[] buffer) throws IOException{
		MatOfByte mob = new MatOfByte(buffer);
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
		
		FeatureDetector siftDetector = FeatureDetector.create(detectorType);
		MatOfKeyPoint mokp = new MatOfKeyPoint();
		siftDetector.detect(image, mokp);
		
		Mat descriptors = new Mat();
		DescriptorExtractor extractor = DescriptorExtractor.create(descriptorType);
		extractor.compute(image, mokp, descriptors);
		return descriptors;
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
			fl.setExtensions(ext);
			try {
				fl.moveTo(location);
			} catch (IOException e) {
				logger.warn("Unable to move to "+location+" due to: "+e.getMessage());
				return new ArrayList<String>();
			}
			return fl.list();
		}else return new ArrayList<String>();
	}

	private Mat feature2Mat(Feature feature){
		List<Descriptor> features = feature.getSparseDescriptors();
		Mat m = new Mat(features.size(), features.get(0).getValues().length, 5);
		for(int r=0; r<features.size(); r++){
			float[] v = features.get(r).getValues();
			for(int c=0; c<v.length; c++){
				m.put(r, c, v[c]);
			}
		}
		return m;
		
	}
}


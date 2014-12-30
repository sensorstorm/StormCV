package nl.tno.stormcv.drpctest;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

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
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.OpenCVOp;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

public class FeatureMatcherOp extends OpenCVOp<Frame> implements ISingleInputOperation<Frame> {

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
	 * Creates a FeatureMatcherOperation using the prototype images located in the provided list. Each URI
	 * in the list is expanded to search for more prototypes (i.e. it is posible to provide a single directory) 
	 * @param prototypes list with URI's pointing to prototype images to be loaded
	 * @param minStrongMatches minimum number of strong matching keypoints
	 * @param minStrongMatchDist
	 */
	public FeatureMatcherOp(List<String> prototypes, int minStrongMatches, float minStrongMatchDist){
		this.protoLocations = prototypes;
		this.minStrongMatches = minStrongMatches;
		this.minStrongMatchDist = minStrongMatchDist;
	}
	
	/**
	 * set the feature detector to use (default = FeatureDetector.SIFT)
	 * @param type
	 * @return
	 */
	public FeatureMatcherOp detector(int type){
		this.detectorType = type;
		return this;
	}
	
	/**
	 * set the feature descriptor to use (default = DescriptorExtractor.SIFT)
	 * @param type
	 * @return
	 */
	public FeatureMatcherOp descriptor(int type){
		this.descriptorType = type;
		return this;
	}
	
	/**
	 * set the matching method to use (default = DescriptorMatcher.BRUTEFORCE)
	 * @param type
	 * @return
	 */
	public FeatureMatcherOp matcher(int type){
		this.matcherType = type;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map conf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(conf);
		matcher = DescriptorMatcher.create( matcherType );
		prototypes = new HashMap<Mat, String>();
		
		for(String location : protoLocations){
			FileConnector fc = connectorHolder.getConnector(location);
			fc.setExtensions(ext);
			fc.moveTo(location);
			List<String> images = fc.list();
			for(String img : images){
				fc.moveTo(img);
				File imageFile = fc.getAsFile();
				Mat proto = calculateDescriptors(ImageIO.read(imageFile));
				prototypes.put(proto, img.substring(img.lastIndexOf('/')+1));
				logger.info("Prototype "+img+" loaded and prepared for matching");
				if(!(fc instanceof LocalFileConnector)) imageFile.delete();
			}
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
		if(!(particle instanceof Frame)) return result;
		
		Frame frame = (Frame)particle;
		if(frame.getImageType().equals(Frame.NO_IMAGE)) return result;
		
		Mat frameDescriptor = this.calculateDescriptors(frame.getImageBytes());
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
				if(!frame.getMetadata().containsKey("strong_matches")){
					frame.getMetadata().put("strong_matches", new String());
				}
				String value = (String)frame.getMetadata().get("strong_matches");
				value +=  prototypes.get(proto)+"("+strongMatches+"), ";
				frame.getMetadata().put("strong_matches", value);
				if(result.size() == 0) result.add(frame);
			}
		}
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

}

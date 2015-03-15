package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.features2d.KeyPoint;
import org.opencv.highgui.Highgui;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * An operation used to detect and describe a wide variety of features using the OpenCV FeatureExtraction and 
 * DescriptorExtractor functions. The name, detector type and extractor type must be provided upon construction.
 * Operation on a single frame results in a single {@link Feature} instance containing a (possibly empty) set of
 * {@link Descriptor}'s. Descriptor length depends on the descriptor type used.
 * 
 * Depending on its configuration this operation can use non-free functions from the OpenCV library which <b><i>may be patented in
 *  some countries or have some other limitations on the use!</i></b> See <a href="http://docs.opencv.org/modules/nonfree/doc/nonfree.html">this page</a>.
 * 
 * @author Corne Versloot
 *
 */
public class FeatureExtractionOp extends OpenCVOp<CVParticle> implements ISingleInputOperation<CVParticle> {

	private static final long serialVersionUID = 3575211578480683490L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private int detectorType;
	private int descriptorType;
	private String featureName;
	private boolean outputFrame = false;
	@SuppressWarnings("rawtypes")
	private CVParticleSerializer serializer = new FeatureSerializer();
	
	/**
	 * @param featureName the name of the feature (i.e. SIFT, SURF, ...) which will be put in the generated Feature's name field
	 * @param detectorType the keypoint detection algorithm to use, must be one of org.opencv.features2d.FeatureDetector constants 
	 * @param descriptorType the type of descriptor to use, must be one of <a href=org.opencv.features2d.DescriptorExtractor constants
	 * @see <a href="http://docs.opencv.org/java/index.html?org/opencv/features2d/FeatureDetector.html">OpenCV FeatureDetector</a>
	 * @see <a href="http://docs.opencv.org/java/index.html?org/opencv/features2d/FeatureDetector.html">OpenCV DescriptorExtractor</a>
	 */
	public FeatureExtractionOp(String featureName, int detectorType, int descriptorType){
		this.featureName = featureName;
		this.detectorType = detectorType;
		this.descriptorType = descriptorType;
	}
	
	/**
	 * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
	 * this Operation will return a {@link Feature} object which means the Frame will no longer be available.
	 * Default value after construction is FALSE.
	 * @param frame
	 * @return
	 */
	public FeatureExtractionOp outputFrame(boolean frame){
		this.outputFrame = frame;
		if(outputFrame){
			this.serializer = new FrameSerializer();
		}else{
			this.serializer = new FeatureSerializer();
		}
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception { }

	@Override
	public void deactivate() {	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return this.serializer;
	}

	@Override
	public List<CVParticle> execute(CVParticle particle) throws Exception {
		List<CVParticle> result = new ArrayList<CVParticle>();
		if(!(particle instanceof Frame)) return result;
		
		Frame frame = (Frame)particle;
		if(frame.getImageType().equals(Frame.NO_IMAGE)) return result;
		try{
			MatOfByte mob = new MatOfByte(frame.getImageBytes());
			Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
			
			FeatureDetector siftDetector = FeatureDetector.create(detectorType);
			MatOfKeyPoint mokp = new MatOfKeyPoint();
			siftDetector.detect(image, mokp);
			List<KeyPoint> keypoints = mokp.toList();
			
			Mat descriptors = new Mat();
			DescriptorExtractor extractor = DescriptorExtractor.create(descriptorType);
			extractor.compute(image, mokp, descriptors);
			List<Descriptor> descrList = new ArrayList<Descriptor>();
			float[] tmp = new float[1];
			for(int r=0; r<descriptors.rows(); r++){
				float[] values = new float[descriptors.cols()];
				for(int c=0; c<descriptors.cols(); c++){
					descriptors.get(r, c, tmp);
					values[c] = tmp[0];
				}
				descrList.add(new Descriptor(frame.getStreamId(), frame.getSequenceNr(), new Rectangle((int)keypoints.get(r).pt.x, (int)keypoints.get(r).pt.y, 0, 0), 0, values));
			}
			
			Feature feature = new Feature(frame.getStreamId(), frame.getSequenceNr(), featureName, 0, descrList, null);
			if(outputFrame){
				frame.getFeatures().add(feature);
				result.add(frame);
			}else{
				result.add(feature);
			}		
		}catch(Exception e){
			// catching exception at this point will prevent the sent of a fail! 
			logger.warn("Unable to extract features for frame!", e);
		}
		return result;
	}

}

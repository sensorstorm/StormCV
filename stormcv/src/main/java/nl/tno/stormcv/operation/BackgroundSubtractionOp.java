package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfPoint;
import org.opencv.core.Rect;
import org.opencv.imgproc.Imgproc;
import org.opencv.video.BackgroundSubtractorMOG;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FeatureSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.util.ImageUtils;

/**
 * Subtracts the background (a history of frames) from a newly provided frame using OpenCV's BackgroundSubtractorMOG.
 * The contours and their bounding box are put into Descriptor objects where the sparse descriptor describes the contour
 * as {x1,y1,x2,y2,..). These descriptors are wrapped in a Feature object with the provided name.
 * The operation returns one of these things:
 * <ul> 
 * <li>Just the Feature object holding all contours (default, outputFrame = false)</li>
 * <li>The original frame with the Feature added (outputFrame = true, asBinaryFrame = false)</li>
 * <li>A binary frame containing the original output of the BackgroundSubtractorMOG with the Feature added (outputFrame = true, asBinaryFrame = true)</li>
 * </ul>
 * @author cversloot
 *
 */
public class BackgroundSubtractionOp extends OpenCVOp<CVParticle> implements ISingleInputOperation<CVParticle>{

	private static final long serialVersionUID = 4901648542417417874L;
	
	private HashMap<String, BackgroundSubtractorMOG> mogs;
	private boolean outputFrame = false;
	private boolean binaryFrame = false;
	private int framesHistory;
	@SuppressWarnings("rawtypes")
	private CVParticleSerializer serializer = new FeatureSerializer();
	private String featureName;

	public BackgroundSubtractionOp(String featureName, int framesHistory) {
		super();
		this.featureName = featureName;
		this.framesHistory = framesHistory;
	}
	
	public BackgroundSubtractionOp outputAsFrame(boolean outputFrame, boolean asBinaryFrame) {
		this.outputFrame = outputFrame;
		this.binaryFrame = asBinaryFrame;
		if(outputFrame){
			this.serializer = new FrameSerializer();
		}else{
			this.serializer = new FeatureSerializer();
		}
		return this;
	}
	
	@Override
	public void deactivate() {
		this.mogs = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return this.serializer;
	}

	@Override
	public List<CVParticle> execute(CVParticle input) throws Exception {
		ArrayList<CVParticle> result = new ArrayList<CVParticle>();
		
		// sanity check
		if( !( input instanceof Frame ) ) 
			return result;
		
		// initialize input and output result
		Frame frame  = (Frame) input;
		String streamId = frame.getStreamId();
			
		// check if input frame has an image
		if( frame.getImageType().equals(Frame.NO_IMAGE) ) 
			return result;

		// decode input image to OpenCV Mat
		Mat inputImage = ImageUtils.bytes2Mat(frame.getImageBytes());
		
		if(!mogs.containsKey(streamId) ){
			mogs.put(streamId, new BackgroundSubtractorMOG());
		}
		// update the background model
		Mat mogMask = new Mat();
	    mogs.get(streamId).apply(inputImage, mogMask, 1f/framesHistory);
	    
    	// find contours for the blobs
    	List<MatOfPoint> contours = new ArrayList<MatOfPoint>();
    	Imgproc.findContours(mogMask.clone(), contours, new Mat(), 0 /*CV_RETR_EXTERNAL*/, 2 /*CV_CHAIN_APPROX_SIMPLE*/);
    	ArrayList<Descriptor> descriptors = new ArrayList<Descriptor>();
    	for(MatOfPoint contour : contours){
    		float[] polygon = new float[contour.rows()*2];
    		for(int row=0; row<contour.rows(); row++){
    			double[] point = contour.get(row, 0);
    			polygon[row*2] = (float)point[0];
    			polygon[row*2+1] = (float)point[1];
    		}
    		Rect rect = Imgproc.boundingRect(contour);
    		if(rect.width < 5 || rect.height < 5 ) continue;
    		descriptors.add(new Descriptor(streamId, input.getSequenceNr(), new Rectangle(rect.x, rect.y, rect.width, rect.height), 0, polygon));
    	}
    	Feature feature = new Feature(streamId, input.getSequenceNr(), featureName, 0, descriptors, null);
    	if(!outputFrame) {
    		result.add(feature);
    		return result;
    	}
    	if(binaryFrame){
    		byte[] outputBytes = ImageUtils.Mat2ImageBytes( mogMask, frame.getImageType() );
            frame.setImage( outputBytes, frame.getImageType() );
    	}
    	frame.getFeatures().add(feature);
    	result.add(frame);
    	return result;
	}

	@Override
	protected void prepareOpenCVOp(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) throws Exception {
		this.mogs = new HashMap<String, BackgroundSubtractorMOG>();		
	}

}

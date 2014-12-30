package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.video.Video;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * Operation to calculate the optical flow between two {@link Frame}s and returns a {@link Feature}
 * with the detected dense optical flow.
 * 
 * @author Corne Versloot
 */

public class OpticalFlowOp extends OpenCVOp<CVParticle> implements IBatchOperation<CVParticle> {

	private static final long serialVersionUID = 998225482249552414L;
	private boolean outputFrame = false;
	@SuppressWarnings("rawtypes")
	private CVParticleSerializer serializer = new FeatureSerializer();
	private String name;

	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception { }
	
	public OpticalFlowOp(String featureName){
		this.name = featureName;
	}
	
	/**
	 * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
	 * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
	 * @param frame
	 * @return
	 */
	public OpticalFlowOp oututFrame(boolean frame){
		this.outputFrame = frame;
		if(outputFrame){
			this.serializer = new FrameSerializer();
		}else{
			this.serializer = new FeatureSerializer();
		}
		return this;
	}
	
	@Override
	public void deactivate() {	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return serializer;
	}

	@Override
	public List<CVParticle> execute(List<CVParticle> input) throws Exception {
		List<CVParticle> result = new ArrayList<CVParticle>();
		if(input.size() != 2 || !(input.get(0) instanceof Frame) || !(input.get(1) instanceof Frame))
			return result;
		
		Frame frame1 = (Frame)input.get(0);
		Frame frame2 = (Frame)input.get(1);
		
		MatOfByte mob1 = new MatOfByte(frame1.getImageBytes());
		Mat image1 = Highgui.imdecode(mob1, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
		Mat image1Gray = new Mat( image1.size(), CvType.CV_8UC1 );
		Imgproc.cvtColor( image1, image1Gray, Imgproc.COLOR_RGB2GRAY );
		
		MatOfByte mob2 = new MatOfByte(frame2.getImageBytes());
		Mat image2 = Highgui.imdecode(mob2, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
		Mat image2Gray = new Mat( image2.size(), CvType.CV_8UC1 );
		Imgproc.cvtColor( image2, image2Gray, Imgproc.COLOR_RGB2GRAY );
		
		Mat opticalFlow = new Mat( image1Gray.size(), CvType.CV_32FC2 );
		Video.calcOpticalFlowFarneback( image1Gray, image2Gray, opticalFlow, 0.5, 1, 1, 1, 7, 1.5, 1 );
		
		int cols = opticalFlow.cols();
		int rows = opticalFlow.rows();
		int maxz = opticalFlow.get(0,0).length;
		float[] tmp = new float[maxz];
		float[][][] dense = new float[cols][rows][maxz];
		for(int y=0; y<opticalFlow.rows(); y++){
			for(int x=0; x<opticalFlow.cols(); x++){
				opticalFlow.get(y,x, tmp);
				dense[x][y][0] = tmp[0];
				dense[x][y][1] = tmp[1];
			}
		}
		
		Feature feature = new Feature(frame1.getRequestId(), frame1.getStreamId(), frame1.getSequenceNr(), name, frame2.getSequenceNr()-frame1.getSequenceNr(), null, dense);
		if(outputFrame){
			frame1.getFeatures().add(feature);
			result.add(frame1);
		}else{
			result.add(feature);
		}

		return result;
	}

}

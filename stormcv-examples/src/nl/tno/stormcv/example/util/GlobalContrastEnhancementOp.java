/**
 * 
 */
package nl.tno.stormcv.example.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.OpenCVOp;
import nl.tno.stormcv.util.ImageUtils;

/**
 * @author John Schavemaker
 *
 */
public class GlobalContrastEnhancementOp extends OpenCVOp<Frame> implements ISingleInputOperation<Frame>
{
	/**
	 * 
	 */
	public enum CEAlgorithm { GRAY_EQUALIZE_HIST, HSV_EQUALIZE_HIST	}

	/**
	 * static class members
	 */
	private static final long serialVersionUID = 1114346630600111394L;

	/**
	 * class members
	 */
	@SuppressWarnings( "rawtypes" )
	private CVParticleSerializer serializer = new FrameSerializer();
	private CEAlgorithm          algorithm  = CEAlgorithm.HSV_EQUALIZE_HIST;

	/**
	 * @return the algorithm
	 */
	public CEAlgorithm getAlgorithm() 
	{
		return algorithm;
	}
	
	/**
	 * @param the algorithm to set
	 */
	public GlobalContrastEnhancementOp setAlgorithm( CEAlgorithm algorithm ) 
	{
		this.algorithm = algorithm;
		return this;
	}
	
	/**
	 * Constructor
	 */
	public GlobalContrastEnhancementOp() 
	{
		super();
	}

	/**
	 * deactivate
	 */
	@Override
	public void deactivate() {}

	/**
	 * getSerializer
	 */
	@Override
	public CVParticleSerializer<Frame> getSerializer() 
	{
		return serializer;
	}

	/**
	 * execute
	 */
	@Override
	public List<Frame> execute( CVParticle input ) throws Exception 
	{
		ArrayList<Frame> result       = new ArrayList<Frame>();
		Frame            input_frame  = null;
		Frame            output_frame = null;
		Mat              input_image  = null;
		Mat              output_image = null;
		
		// sanity check
		if ( !( input instanceof Frame ) ) 
			return result;

		// initialize input and output result
		input_frame  = (Frame) input;
		output_frame = input_frame;
		output_image = new Mat();

		// check if input has an image
		if( input_frame.getImageType().equals( Frame.NO_IMAGE ) ) 
			return result;

		// decode input image byte array to OpenCV Mat
		input_image = ImageUtils.bytes2Mat( input_frame.getImageBytes() );

		// convert to gray and equalize histogram
		if ( algorithm == CEAlgorithm.GRAY_EQUALIZE_HIST )
		{
			Imgproc.cvtColor( input_image, output_image, Imgproc.COLOR_BGR2GRAY );
			Imgproc.equalizeHist( output_image, output_image );
		}
		else if ( algorithm == CEAlgorithm.HSV_EQUALIZE_HIST )
		{
			List<Mat> hsv_channels = new ArrayList<Mat>(3);
			Mat       hsv_image    = new Mat();
			Mat       enhanced_val = new Mat();

			// change color space
			Imgproc.cvtColor( input_image, hsv_image, Imgproc.COLOR_BGR2HSV );

			// get color channels
			Core.split(hsv_image, hsv_channels);
			Mat hue = hsv_channels.get( 0 );
			Mat sat = hsv_channels.get( 1 );
			Mat val = hsv_channels.get( 2 );

			// equalize histogram of value channel
			Imgproc.equalizeHist( val, enhanced_val );

			// replace value channel with new one
			hsv_channels.set( 2, enhanced_val );

			// merge channels together again
			Core.merge( hsv_channels, hsv_image );

			// go back to original color space
			Imgproc.cvtColor( hsv_image, output_image, Imgproc.COLOR_HSV2BGR );
		}

		// convert output image to byte array and set image in output frame
		byte[] outputBytes = ImageUtils.Mat2ImageBytes(output_image, input_frame.getImageType());
		output_frame.setImage(outputBytes, input_frame.getImageType());
		result.add( output_frame );

		return result;
	}

	/**
	 * prepareOpenCVOp
	 */
	@Override
	protected void prepareOpenCVOp( Map arg0, TopologyContext arg1 )
			throws Exception {}
}


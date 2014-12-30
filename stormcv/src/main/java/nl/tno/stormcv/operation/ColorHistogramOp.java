package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfInt;
import org.opencv.core.MatOfFloat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.FeatureSerializer;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;

/**
 * Operation to calculate the color histogram of a {@link Frame} and returns a {@link Feature} 
 * with a histogram per color channel.
 * 
 * @author Corne Versloot
 */

public class ColorHistogramOp extends OpenCVOp<Feature> implements ISingleInputOperation<Feature>{
	
	private static final long serialVersionUID = -5543735411296339252L;
	private String name;
	private int[] chansj = new int[]{0,1,2};
	private int[] histsizej = new int[]{255, 255, 255};
	private float[] rangesj = new float[]{0, 256, 0, 256, 0, 256 }; 


	public ColorHistogramOp(String name){
        this.name = name;
	}
	
	/**
	 * Configure the HistorgramOperation. The default is set for use of RGB images
	 * @param chans list with channal id's default = {0, 1, 2}
	 * @param histsize for each channel the number of bins to use, default = {255, 255 ,255}
	 * @param ranges for each channel the min. and max. values present, default = {0, 256, 0, 256, 0, 256 }
	 * @see <a href="http://docs.opencv.org/2.4.8/modules/imgproc/doc/histograms.html">OpenCV Documentation</a>
	 */
	public ColorHistogramOp configure(int[] chans, int[] histsize, float[] ranges){
        chansj = chans;
        histsizej = histsize;
        rangesj = ranges;
        return this;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception {	}
	
	@Override
	public List<Feature> execute(CVParticle input) throws Exception 
	{
		Frame sf = (Frame)input;
		MatOfByte mob = new MatOfByte(sf.getImageBytes());
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
		Mat hist = new Mat();

        MatOfInt chans;
        MatOfInt histsize;
        MatOfFloat ranges;

        List<Mat> images = new ArrayList<Mat>();

		ArrayList<Feature>    result                = new ArrayList<Feature>();
		ArrayList<Descriptor> hist_descriptors      = new ArrayList<Descriptor>();
		
        Rectangle box = new Rectangle(0, 0, (int) image.size().width, (int) image.size().height); // size of image get boundingbox from sf

        images.add(image);
        for (int i = 0; i < chansj.length; i++){
            chans = new MatOfInt(chansj[i]);
            histsize = new MatOfInt(histsizej[i]);
            ranges = new MatOfFloat(rangesj[i*2],rangesj[i*2+1]);
            Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

            float[] tmp = new float[1];

            int rows = (int) hist.size().height;
            
            float[] values = new float[rows];
            int c = 0;
            for (int r = 0; r < rows; r++) // loop over rows/columns
            {
                hist.get(r, c, tmp);
                values[r] = tmp[0];
            }
            hist_descriptors.add(new Descriptor(input.getRequestId(), input.getStreamId(), input.getSequenceNr(), box, 0, values));
        }
		
				
		// add features to result
		if ( hist_descriptors.size() > 0 )
			result.add( new Feature( input.getRequestId(), input.getStreamId(), input.getSequenceNr(), name, 0, hist_descriptors, null ) );
		
		return result;
	}

	@Override
	public void deactivate() 
	{
		
	}

	@Override
	public CVParticleSerializer<Feature> getSerializer() 
	{
		return new FeatureSerializer();
	}

}

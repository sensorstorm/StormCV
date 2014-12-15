package nl.tno.stormcv.operation;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * Scales an image into a new image. The original java drawImage function using RenderingHints is used
 * to balance speed vs quality. The approach is taken from <a href="https://today.java.net/pub/a/today/2007/04/03/perils-of-image-getscaledinstance.html">here</a>
 * Hence scaling up is done using VALUE_INTERPOLATION_BICUBIC setting and scaling down with VALUE_INTERPOLATION_BILINEAR. 
 * Down scaling is done recursively if the scale factor is smaller than 0.5.
 * 
 * @author Corne Versloot
 *
 */
public class ScaleImageOp implements ISingleInputOperation<Frame>{

	private static final long serialVersionUID = -8518918556266893201L;

	private float factor;
	
	/**
	 * Creates a ScaleOperation that will scale images usint the provided factor. Using 
	 * a factor of 1.0 will have no difference (except for wasting CPU cycles)
	 * @param factor
	 */
	public ScaleImageOp(float factor){
		this.factor = factor;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf , TopologyContext context) throws Exception {	}

	@Override
	public void deactivate() {	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		if(!(particle instanceof Frame)) return result;
		
		Frame frame = (Frame) particle;
		BufferedImage image = frame.getImage();
		if(image == null) return result;
		
		if(factor != 1.0) image = ScaleImageOp.scale(image, factor);
		frame.setImage(image);
		result.add(frame);
		return result;
	}
	
	private static BufferedImage scale(BufferedImage original, float factor){
		BufferedImage newImage;
		if(factor > 1){
			newImage = new BufferedImage((int)Math.round(original.getWidth()*factor), (int)Math.round(original.getHeight()*factor), original.getType());
			Graphics2D g2 = newImage.createGraphics();
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            g2.drawImage(original, 0, 0, newImage.getWidth(), newImage.getHeight(), null);
            g2.dispose();
            return newImage;
		}else if(factor > 0.5){
			newImage = new BufferedImage((int)Math.round(original.getWidth()*factor), (int)Math.round(original.getHeight()*factor), original.getType());
			Graphics2D g2 = newImage.createGraphics();
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2.drawImage(original, 0, 0, newImage.getWidth(), newImage.getHeight(), null);
            g2.dispose();
            return newImage;
		}else{
			float currentFactor = 0.5f;
			newImage = new BufferedImage((int)Math.round(original.getWidth()*currentFactor), (int)Math.round(original.getHeight()*currentFactor), original.getType());
			Graphics2D g2 = newImage.createGraphics();
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2.drawImage(original, 0, 0, newImage.getWidth(), newImage.getHeight(), null);
            g2.dispose();
            return scale(newImage, factor/currentFactor);
		}
	}

}

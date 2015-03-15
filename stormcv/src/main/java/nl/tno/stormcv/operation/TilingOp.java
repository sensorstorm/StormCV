package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * This Operation splits a provided frame/image into a configured number of rectangular tiles. Each tile will have
 * the original streamId concatenated with the tile index. For example a frame with stream 'abcd' split into 2x2 tiles
 * will result in 4 frames with streamId: 'abcd_0' (top-left), 'abcd_1' (top-right), 'abcd_2' (bottom-left), 'abcd_3' (bottom-right).
 * Hence the tiler operation will also slit the stream which is important for grouping purposes! The Frame's boundingBox provides
 * exact information on the position of the tile.
 * 
 * It is possible to specify a number of pixels neighboring tiles must overlap (default = 0px). 
 * 
 * @author Corne Versloot
 *
 */
public class TilingOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = 1235734323465856261L;
	private FrameSerializer serialzier = new FrameSerializer();
	private int rows = 2;
	private int cols = 2;
	private int pixelOverlap = 0;
	private String imageType;
	
	public TilingOp(int rows, int cols){
		this.rows = rows;
		this.cols = cols;
	}
	
	public TilingOp overlap(int pixels){
		this.pixelOverlap = pixels;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) throws Exception {
		if(conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)){
			imageType = (String)conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
		}
	}

	@Override
	public void deactivate() {	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serialzier;
	}

	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		if(!(particle instanceof Frame)) return result;
		
		Frame frame = (Frame) particle;
		BufferedImage image = frame.getImage();
		if(image == null) return result;
		if(image.getWidth()<2*cols || image.getHeight()<2*rows) return result;
		
		int width = image.getWidth() / cols;
		int height = image.getHeight() / rows;
		int tileIndex = 0;
		for(int r=0; r<rows; r++){
			for(int c=0; c<cols; c++){
				Rectangle box = new Rectangle(c*width, r*height, width + pixelOverlap, height + pixelOverlap);
				box = box.intersection(frame.getBoundingBox());
				BufferedImage tile = image.getSubimage(box.x, box.y, box.width, box.height);
				byte[] buffer = ImageUtils.imageToBytes(tile, imageType);
				result.add(new Frame(frame.getStreamId()+"_"+tileIndex, frame.getSequenceNr(), imageType, buffer, frame.getTimestamp(), box));
				tileIndex++;
			}
		}
		return result;
	}
}

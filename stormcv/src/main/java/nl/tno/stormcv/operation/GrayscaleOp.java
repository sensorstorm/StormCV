package nl.tno.stormcv.operation;

import java.awt.image.ColorConvertOp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * A simple operation that converts the image within received {@link Frame} objects to gray
 * using Java's native {@link ColorConvertOp}. Hence the OpenCV library is not required to use this Operation.
 * 
 * @author Corne Versloot
 *
 */
public class GrayscaleOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = 1254502507730636800L;
	private FrameSerializer serializer = new FrameSerializer();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) { }

	@Override
	public List<Frame> execute(CVParticle particle) throws IOException {
		List<Frame> result = new ArrayList<Frame>();
		Frame sf = (Frame)particle;
		sf.setImage(ImageUtils.convertToGray(sf.getImage()));
		result.add(sf);
		return result;
	}

	@Override
	public void deactivate() { }

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}

}

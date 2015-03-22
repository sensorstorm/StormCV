package nl.tno.stormcv.example.drpc;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.drpc.IRequestOp;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

/**
 * 
 * Special operation that receives drpc requests. This implementation of IRequestOp simply
 * gets a pointer to an image file, reads the image and returns it as a frame.
 * 
 * @author Corne Versloot
 *
 */
public class FeatureMatchRequestOp implements IRequestOp<Frame> {

	private static final long serialVersionUID = 5975348799605975615L;
	private ConnectorHolder connectorHolder;

	@Override
	public void deactivate() {	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(conf);
	}

	@Override
	public List<Frame> execute(long requestId, String templateImage) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		FileConnector connector = connectorHolder.getConnector(templateImage);
		if(connector == null) throw new FileNotFoundException("No image found for argument: "+templateImage);
		
		connector.moveTo(templateImage);
		File imageFile = connector.getAsFile();
		BufferedImage image = ImageIO.read(imageFile);
		Frame queryFrame = new Frame("nostream", 0, Frame.JPG_IMAGE, image, 0, new Rectangle(0,0,image.getWidth(), image.getHeight()));
		queryFrame.getMetadata().put("uri", templateImage);
		if(!(connector instanceof LocalFileConnector)) imageFile.delete();
		result.add(queryFrame);
		return result;
	}

}

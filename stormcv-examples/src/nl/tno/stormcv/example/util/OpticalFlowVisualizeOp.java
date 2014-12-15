package nl.tno.stormcv.example.util;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.ISingleInputOperation;

public class OpticalFlowVisualizeOp implements ISingleInputOperation<Frame>{

	private static final long serialVersionUID = 1183401329954092073L;
	private String featureName;

	public OpticalFlowVisualizeOp(String featureName) {
		this.featureName = featureName;
	}

	@Override
	public void deactivate() {	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1) throws Exception {	}

	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		if(!(particle instanceof Feature) ) return result;
		Feature of = (Feature)particle;
		if(!of.getName().equals(featureName)) return result;
		
		float[][][] dense = of.getDenseDescriptors();
		BufferedImage image = new BufferedImage(dense.length, dense[0].length, BufferedImage.TYPE_INT_RGB);
		
		for(int x=0; x<dense.length; x++){
			for(int y=0; y<dense[x].length; y++){
				float v = (float) Math.sqrt(Math.pow(dense[x][y][0],2) + Math.pow(dense[x][y][1], 2)); 
				v = Math.min(v, 1);
				image.setRGB(x, y, new Color(v, v, v).getRGB());
			}
		}
		result.add(new Frame(of.getStreamId(), of.getSequenceNr(), Frame.JPG_IMAGE, image, 0, new Rectangle(0,0,image.getWidth(), image.getHeight())));
		return result;
	}

}

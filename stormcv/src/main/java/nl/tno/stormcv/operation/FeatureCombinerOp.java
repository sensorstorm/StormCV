package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.batcher.SequenceNrBatcher;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * A batch operation that wraps multiple {@link Feature} objects from multiple bolts in a single {@link Frame} without an image.
 * This operation is typically used in combination with the {@link SequenceNrBatcher} which will batch received Features on their sequence number. 
 * If the {@link FeatureCombinerOp} for example as to combine three features (faces, SIFT, SURF) coming from three different bolts 
 * the Batcher will be instantiated as <code>new SequenceNrBatcher(3)</code> 
 * 
 * This operation will try to merge features if they have the same name simply by combining their sparse {@link Descriptor}'s. In other words;
 * if two SIFT Feature's are received for the exact same frame (same streamId and sequenceNr) their descriptors will simply be combined but 
 * no check is made if they are the same or not. 
 * 
 * @author Corne Versloot
 *
 */
public class FeatureCombinerOp implements IBatchOperation<Frame> {

	private static final long serialVersionUID = -2694295477863874065L;
	private FrameSerializer serializer = new FrameSerializer();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws Exception { }

	@Override
	public void deactivate() { }

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}

	@Override
	public List<Frame> execute(List<CVParticle> input) throws Exception {
		Frame frame = null;
		List<Feature> features = new ArrayList<Feature>();
		for(CVParticle particle : input){
			if(particle instanceof Feature){
				features.add((Feature)particle);
			}else if(particle instanceof Frame && frame == null){
				frame = (Frame) particle;
			}
		}
		if(frame == null) frame = new Frame(input.get(0).getStreamId(), input.get(0).getSequenceNr(), Frame.NO_IMAGE, (byte[])null, 0L, new Rectangle());
		
		// merge new features with already existing features in the Frame
		f1 : for(Feature newF : features){
			for(Feature oldF : frame.getFeatures()){
				if(newF.getName().equals(oldF.getName())){
					oldF.getSparseDescriptors().addAll(newF.getSparseDescriptors());
					continue f1;
				}
			}
			frame.getFeatures().add(newF);
		}
		List<Frame> result = new ArrayList<Frame>();
		result.add(frame);
		return result;
		
	}

}

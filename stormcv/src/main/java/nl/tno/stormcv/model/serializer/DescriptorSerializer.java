package nl.tno.stormcv.model.serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.awt.Rectangle;

import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.CVParticle;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DescriptorSerializer extends CVParticleSerializer<Descriptor> implements Serializable {

	private static final long serialVersionUID = 9035480740735532324L;
	public static final String BOUNDINGBOX = FrameSerializer.BOUNDINGBOX;
	public static final String DURATION = FeatureSerializer.DURATION;
	public static final String VALUES = "values";
	
	
	@Override
	protected Descriptor createObject(Tuple tuple) throws IOException {
		return new Descriptor(tuple, (Rectangle)tuple.getValueByField(BOUNDINGBOX), (Long)tuple.getValueByField(DURATION), (float[])tuple.getValueByField(VALUES));
	}

	@Override
	protected Values getValues(CVParticle particle) throws IOException {
		Descriptor descriptor = (Descriptor) particle;
		return new Values(descriptor.getBoundingBox(), descriptor.getDuration(), descriptor.getValues());
	}

	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(BOUNDINGBOX);
		fields.add(DURATION);
		fields.add(VALUES);
		return fields;
	}

	@Override
	protected void writeObject(Kryo kryo, Output output, Descriptor descriptor) {
		output.writeFloat((float)descriptor.getBoundingBox().getX());
		output.writeFloat((float)descriptor.getBoundingBox().getY());
		output.writeFloat((float)descriptor.getBoundingBox().getWidth());
		output.writeFloat((float)descriptor.getBoundingBox().getHeight());
		
		output.writeLong(descriptor.getDuration());
		
		output.writeInt(descriptor.getValues().length);
		for(Float f : descriptor.getValues()){
			output.writeFloat(f);
		}
	}

	@Override
	protected Descriptor readObject(Kryo kryo, Input input, Class<Descriptor> clas, String streamId, long sequenceNr) {
		Rectangle rectangle = new Rectangle(Math.round(input.readFloat()), Math.round(input.readFloat()), 
				Math.round(input.readFloat()), Math.round(input.readFloat()));
		long duration = input.readLong();
		int length = input.readInt();
		float[] values = new float[length];
		for(int i=0; i<length; i++){
			values[i] = input.readFloat();
		}
		return new Descriptor(streamId, sequenceNr, rectangle, duration, values);
	}

}

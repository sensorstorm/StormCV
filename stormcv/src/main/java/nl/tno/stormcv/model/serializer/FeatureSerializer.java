package nl.tno.stormcv.model.serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.CVParticle;

public class FeatureSerializer extends CVParticleSerializer<Feature> implements Serializable {

	private static final long serialVersionUID = 3121908157999500459L;
	public static final String NAME = "name";
	public static final String DURATION = "duration";
	public static final String SPARSE_DESCR = "sparse";
	public static final String DENSE_DESCR = "dense";

	@SuppressWarnings("unchecked")
	@Override
	protected Feature createObject(Tuple tuple) throws IOException {
		List<Descriptor> sparseDescriptors = (List<Descriptor>) tuple.getValueByField(SPARSE_DESCR);
		float[][][] denseDescriptors = (float[][][])tuple.getValueByField(DENSE_DESCR);
		Feature feature = new Feature(tuple, tuple.getStringByField(NAME), tuple.getLongByField(DURATION), sparseDescriptors, denseDescriptors);
		return feature;
	}

	@Override
	protected Values getValues(CVParticle particle) throws IOException {
		Feature feature = (Feature)particle;
		return new Values(feature.getName(), feature.getDuration(), feature.getSparseDescriptors(), feature.getDenseDescriptors());
	}

	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(NAME);
		fields.add(DURATION);
		fields.add(SPARSE_DESCR);
		fields.add(DENSE_DESCR);
		return fields;
	}
	
	@Override
	protected void writeObject(Kryo kryo, Output output, Feature feature) throws Exception {
		output.writeString(feature.getName());
		output.writeLong(feature.getDuration());
		kryo.writeObject(output, feature.getSparseDescriptors());
		float[][][] m = feature.getDenseDescriptors();
		output.writeInt(m.length); // write x
		if(m.length == 0) return;
		
		output.writeInt(m[0].length); // write y
		output.writeInt(m[0][0].length); // write z
		for(int x=0; x<m.length; x++){
			for(int y=0; y<m[0].length; y++){
				for(int z=0; z<m[0][0].length; z++){
					output.writeFloat(m[x][y][z]);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Feature readObject(Kryo kryo, Input input, Class<Feature> clas, long requestId, String streamId, long sequenceNr) throws Exception {
		String name = input.readString();
		long duration = input.readLong();
		List<Descriptor> sparseDescriptors = kryo.readObject(input, ArrayList.class);
		
			int xl = input.readInt();
			float[][][] denseDescriptor = null;
			if(xl > 0){
				int yl = input.readInt();
				int zl = input.readInt();
				denseDescriptor = new float[xl][yl][zl];
				for(int x=0; x<xl; x++){
					for(int y=0; y<yl; y++){
						for(int z=0; z<zl; z++){
							denseDescriptor[x][y][z] = input.readFloat();
						}
					}
				}
			}
		
		Feature feature = new Feature(requestId, streamId, sequenceNr, name, duration, sparseDescriptors, denseDescriptor);
		return feature;
	}

}

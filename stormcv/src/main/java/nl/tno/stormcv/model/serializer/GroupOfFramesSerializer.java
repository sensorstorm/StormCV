package nl.tno.stormcv.model.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.CVParticle;

public class GroupOfFramesSerializer extends CVParticleSerializer<GroupOfFrames> {
	
	public static final String FRAME_LIST = "frame_list";

	@SuppressWarnings("unchecked")
	@Override
	protected GroupOfFrames createObject(Tuple tuple) throws IOException {
		return new GroupOfFrames(tuple, (List<Frame>)tuple.getValueByField(FRAME_LIST));
	}

	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(FRAME_LIST);
		return fields;
	}

	@Override
	protected Values getValues(CVParticle particle) throws IOException {
		GroupOfFrames mf = (GroupOfFrames)particle;
		return new Values(mf.getFrames());
	}

	@Override
	protected GroupOfFrames readObject(Kryo kryo, Input input,	Class<GroupOfFrames> clas, String streamId, long sequenceNr) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void writeObject(Kryo arg0, Output arg1, GroupOfFrames arg2) throws Exception {
		// TODO Auto-generated method stub
		
	}

}

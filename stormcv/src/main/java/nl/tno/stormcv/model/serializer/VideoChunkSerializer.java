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

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.VideoChunk;


public class VideoChunkSerializer extends CVParticleSerializer<VideoChunk> implements Serializable{

	private static final long serialVersionUID = 7864791957493203664L;

	public static final String VIDEO = "video";
	public static final String TIMESTAMP = "timeStamp";
	
	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(TIMESTAMP);
		fields.add(VIDEO);
		return fields;
	}
	
	@Override
	protected Values getValues(CVParticle object) throws IOException {
		VideoChunk video = (VideoChunk)object;
		return new Values(video.getDuration(), video.getVideo());
	}
	
	@Override
	protected VideoChunk createObject(Tuple tuple) throws IOException {
		return new VideoChunk(tuple, tuple.getLongByField(TIMESTAMP), tuple.getBinaryByField(VIDEO));
	}
	
	@Override
	protected void writeObject(Kryo kryo, Output output, VideoChunk video) throws Exception {
		output.writeLong(video.getDuration());
		output.writeInt(video.getVideo().length);
		output.writeBytes(video.getVideo());
	}

	@Override
	protected VideoChunk readObject(Kryo kryo, Input input, Class<VideoChunk> clas, String streamId, long sequenceNr) throws Exception {
		long duration = input.readLong();
		int length = input.readInt();
		byte[] video = input.readBytes(length);
		return new VideoChunk(streamId, sequenceNr, duration, video);
		
	}



}

package nl.tno.stormcv.model.serializer;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.CVParticle;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FrameSerializer extends CVParticleSerializer<Frame> implements Serializable{

	private static final long serialVersionUID = 1952141838734994463L;
	public static final String IMAGETYPE = "imagetype";
	public static final String IMAGE = "imagebytes";
	public static final String TIMESTAMP = "timeStamp";
	public static final String BOUNDINGBOX = "boundingbox";
	public static final String FEATURES = "features";

	@SuppressWarnings("unchecked")
	@Override
	protected Frame createObject(Tuple tuple) throws IOException {
		byte[] buffer = tuple.getBinaryByField(IMAGE);
		Frame frame;
		if(buffer == null){
			frame = new Frame(tuple, tuple.getStringByField(IMAGETYPE), null, tuple.getLongByField(TIMESTAMP), (Rectangle)tuple.getValueByField(BOUNDINGBOX));
		}else{
			frame = new Frame(tuple, tuple.getStringByField(IMAGETYPE), buffer, tuple.getLongByField(TIMESTAMP), (Rectangle)tuple.getValueByField(BOUNDINGBOX));
		}
		frame.getFeatures().addAll((List<Feature>)tuple.getValueByField(FEATURES));
		return frame;
	}

	@Override
	protected Values getValues(CVParticle particle) throws IOException {
		Frame frame = (Frame)particle;
		BufferedImage image = frame.getImage();
		if(image == null){
			return new Values(frame.getImageType(), (Object[])null, frame.getTimestamp(), frame.getBoundingBox(), frame.getFeatures());
		}else{
			return new Values(frame.getImageType(), frame.getImageBytes(), frame.getTimestamp(), frame.getBoundingBox(), frame.getFeatures());
		}
	}

	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(IMAGETYPE);
		fields.add(IMAGE);
		fields.add(TIMESTAMP);
		fields.add(BOUNDINGBOX);
		fields.add(FEATURES);
		return fields;
	}

	@Override
	protected void writeObject(Kryo kryo, Output output, Frame frame) throws IOException{
		output.writeLong(frame.getTimestamp());
		output.writeString(frame.getImageType());
		byte[] buffer = frame.getImageBytes();
		if(buffer != null){
			output.writeInt(buffer.length);
			output.writeBytes(buffer);
		}else{
			output.writeInt(0);
		}
		output.writeFloat((float)frame.getBoundingBox().getX());
		output.writeFloat((float)frame.getBoundingBox().getY());
		output.writeFloat((float)frame.getBoundingBox().getWidth());
		output.writeFloat((float)frame.getBoundingBox().getHeight());
		
		kryo.writeObject(output, frame.getFeatures());
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Frame readObject(Kryo kryo, Input input, Class<Frame> clas, long requestId, String streamId, long sequenceNr) throws IOException{
		long timeStamp = input.readLong();
		String imageType = input.readString();
		int buffSize = input.readInt();
		byte[] buffer = null;
		if(buffSize > 0){
			buffer = new byte[buffSize];
			input.readBytes(buffer);
		}
		Rectangle boundingBox = new Rectangle(Math.round(input.readFloat()), Math.round(input.readFloat()), 
				Math.round(input.readFloat()), Math.round(input.readFloat()));
		List<Feature> features = kryo.readObject(input, ArrayList.class);
		
		return new Frame(requestId, streamId, sequenceNr, imageType, buffer, timeStamp, boundingBox, features);
	}

}

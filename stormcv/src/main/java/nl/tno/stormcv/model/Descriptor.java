package nl.tno.stormcv.model;

import java.awt.Rectangle;

import backtype.storm.tuple.Tuple;

/**
 * This {@link CVParticle} implementation represents a sparse descriptor which is part of a {@link Feature} and has the following fields:
 * <ul>
 * <li>boundinbBox: an optionally zero dimensional rectangle indicating the region in the image this Descriptor describes</li>
 * <li>duration: the duration of the descriptor which may apply to temporal features like STIP</li>
 * <li>values: the float array used to store the actual descriptor (for example 128 values describing a SIFT point)</li>
 * </ul>  
 * 
 * @author Corne Versloot
 *
 */
public class Descriptor extends CVParticle {

	private Rectangle boundingBox;
	private long duration;
	private float[] values;
	
	public Descriptor(long requestId, String streamId, long sequenceNr, Rectangle boundingBox, long duration, float[] values) {
		super(requestId, streamId, sequenceNr);
		this.boundingBox = boundingBox;
		this.values = values;
		this.duration = duration;
	}
	
	public Descriptor(Tuple tuple, Rectangle boundingBox, long duration, float[] values) {
		super(tuple);
		this.boundingBox = boundingBox;
		this.duration = duration;
		this.values = values;
	}

	public Rectangle getBoundingBox() {
		return boundingBox;
	}

	public void setBoundingBox(Rectangle box){
		this.boundingBox = box;
	}
	
	public long getDuration(){
		return duration;
	}

	public float[] getValues() {
		return values;
	}
	
	/**
	 * Simply changes the location of this descriptor by moving it in the provided x,y direction
	 * @param x
	 * @param y
	 */
	public void translate(int x, int y){
		this.boundingBox.x += x;
		this.boundingBox.y += y;
	}
	
	public Descriptor deepCopy(){
		float[] valuesCopy = new float[values.length];
		for(int i=0;i<values.length; i++){
			valuesCopy[i] = values[i];
		}
		Descriptor copy = new Descriptor(getRequestId(), new String(this.getStreamId()), this.getSequenceNr(), new Rectangle(this.getBoundingBox()), this.getDuration(), valuesCopy);
		copy.setMetadata(this.getMetadata());
		return copy;
	}
	
	public String toString(){
		return "Descriptor {stream:"+getStreamId()+", nr:"+getSequenceNr()+", box:"+boundingBox+" duration: "+duration+"}";
	}
}
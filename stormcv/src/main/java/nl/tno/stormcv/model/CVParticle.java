package nl.tno.stormcv.model;

import java.util.HashMap;

import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import backtype.storm.tuple.Tuple;

/**
 * An abstract Computer Vision Particle superclass which contains the information required
 * for all objects processed by the platform:
 * <ul>
 * <li>streamId: an identifier of the stream a piece of information refers to</li>
 * <li>sequenceNr: the number within a stream a piece of information refers to (typically the frame number of a video stream/file)</li>
 * <li>metadata: Map&lt;String,Object&gt; that can be used to for other information</li>
 * <li>requestId: the id required by DRPC topologies</li>
 * </ul>
 * Most of the standard components within StormCV make use of the streamId and sequenceNr to group, filter and order objects they get.
 * 
 * CVParticles are compared based on their sequenceNr (ascending order). 
 *  
 * @author Corne Versloot
 *
 */
public abstract class CVParticle implements Comparable<CVParticle>{
	
	private Tuple tuple;
	private long requestId;
	private String streamId;
	private long sequenceNr;
	private HashMap<String, Object> metadata = new HashMap<String, Object>();
	
	/**
	 * Constructs a generic type based on the provided tuple. The tuple must contain streamID and sequenceNR
	 * values. 
	 * @param tuple
	 */
	@SuppressWarnings("unchecked")
	public CVParticle(Tuple tuple){
		this(tuple.getLongByField(CVParticleSerializer.REQUESTID), 
				tuple.getStringByField(CVParticleSerializer.STREAMID), 
				tuple.getLongByField(CVParticleSerializer.SEQUENCENR));
		this.tuple = tuple;
		this.setMetadata((HashMap<String, Object>)tuple.getValueByField(CVParticleSerializer.METADATA));
	}
	
	/**
	 * Constructs a GenericType object for some piece of information regarding a stream
	 * @param streamId the id of the stream
	 * @param sequenceNr the sequence number of the the information (used for ordering)
	 */
	public CVParticle(long requestId, String streamId, long sequenceNr){
		this.requestId = requestId;
		this.streamId = streamId;
		this.sequenceNr = sequenceNr;
	}
	
	public String getStreamId(){
		return streamId;
	}
	
	public long getSequenceNr(){
		return sequenceNr;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public HashMap<String, Object> getMetadata() {
		return metadata;
	}
	
	public void setMetadata(HashMap<String, Object> metadata) {
		if(metadata != null){
			this.metadata = metadata;
		}
	}
	
	public long getRequestId() {
		return requestId;
	}
	
	/**
	 * Compares one generictype to another based on their sequence number.
	 * @return -1 if this.sequenceNr < other.sequenceNr, 0 if this.sequenceNr == other.sequenceNr else 1
	 */
	@Override
	public int compareTo(CVParticle other){
		return (int)(getSequenceNr() - other.getSequenceNr());
	}


}

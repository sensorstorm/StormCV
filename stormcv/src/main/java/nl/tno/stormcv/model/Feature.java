package nl.tno.stormcv.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Tuple;

/**
 * This {@link CVParticle} implementation represents a single feature calculated for {@link Frame} and has the following fields:
 * <ul>
 * <li>name: the name of the feature like 'SIFT', 'SURF', 'HOG' etc</li>
 * <li>duration: the duration of the feature in case it describes a temporal aspect of multiple frames</li>
 * <li>sparseDescriptors: a list with {@link Descriptor} objects used to described sparse features like SIFT</li>
 * <li>denseDescriptors: a three dimensional float array much like the OpenCV Mat object which can be used to represent 
 * dense features like dense Optical Flow</li>
 * </ul>
 * It is not always clear how a specific descriptor should be stored and it is typically up to the characteristics of the 
 * topology and context what is the best way to go.  
 *  
 * @author Corne Versloot
 *
 */
public class Feature extends CVParticle{

	private String name;
	private long duration;
	private List<Descriptor> sparseDescriptors = new ArrayList<Descriptor>();
	private float[][][] denseDescriptors = new float[0][0][0];
	
	public Feature(String streamId, long sequenceNr, String name, long duration, List<Descriptor> sparseDescriptors, float[][][] denseDescriptors) {
		super(streamId, sequenceNr);
		this.name = name;
		this.duration = duration;
		if(sparseDescriptors != null){
			this.sparseDescriptors = sparseDescriptors;
		}
		if(denseDescriptors != null){
			this.denseDescriptors = denseDescriptors;
		}
	}
	
	public Feature(Tuple tuple, String name, long duration, List<Descriptor> sparseDescriptors, float[][][] denseDescriptors) {
		super(tuple);
		this.name = name;
		this.duration = duration;
		if(sparseDescriptors != null){
			this.sparseDescriptors = sparseDescriptors;
		}
		if(denseDescriptors != null){
			this.denseDescriptors = denseDescriptors;
		}
	}

	public String getName() {
		return name;
	}

	public List<Descriptor> getSparseDescriptors() {
		return sparseDescriptors;
	}
	
	public float[][][] getDenseDescriptors(){
		return denseDescriptors;
	}
	
	public long getDuration(){
		return this.duration;
	}
	
	public Feature deepCopy(){
		float[][][] denseCopy = new float[denseDescriptors.length][][];
		for(int x=0; x<denseDescriptors.length; x++){
			denseCopy[x] = new float[denseDescriptors[x].length][];
			for(int y=0; y<denseDescriptors[x].length; y++){
				denseCopy[x][y] = Arrays.copyOf(denseDescriptors[x][y], denseDescriptors[x][y].length);
			}
		}
		
		List<Descriptor> sparseCopy = new ArrayList<Descriptor>(this.sparseDescriptors.size());
		for(Descriptor d : sparseDescriptors){
			sparseCopy.add(d.deepCopy());
		}
		
		Feature copyFeature = new Feature(new String(this.getStreamId()), this.getSequenceNr(), new String(this.getName()), this.getDuration(), 
				sparseCopy, denseCopy);
		copyFeature.setRequestId(getRequestId());
		copyFeature.setMetadata(this.getMetadata());
		return copyFeature;
	}
	
	public String toString(){
		return "Feature {stream:"+getStreamId()+", nr:"+getSequenceNr()+", name: "+name+", descriptors: "+sparseDescriptors+"}";
	}
}

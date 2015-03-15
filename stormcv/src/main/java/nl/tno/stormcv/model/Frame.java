package nl.tno.stormcv.model;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.util.ImageUtils;

import backtype.storm.tuple.Tuple;

/**
 * A {@link CVParticle} object representing a frame from a video stream (or part of a frame called a Tile). Additional to the fields inherited from
 * {@link CVParticle} the Frame object has the following fields:
 * <ul>
 * <li>timeStamp: relative timestamp in ms of this frame with respect to the start of the stream (which is 0 ms)</li>
 * <li>image: the actual frame represented as BufferedImage <i>(may be NULL)</i></li>
 * <li>boundingBox: indicates which part of the original frame this object represents. This might be the full frame but can also be 
 * some other region of interest. This box can be used for tiling and combining purposes as well.</li>
 * <li>features: list with {@link Feature} objects (containing one or more {@link Descriptor}) describing 
 * aspects of the frame such as SIFT, color histogram or Optical FLow (list may be empty)</li>
 * </ul>
 * 
 * A Frame can simply function as a container for multiple Features and hence may not actually have an image. In this case getImageType will 
 * return Frame.NO_IMAGE and getImage will return null.
 * 
 * @author Corne Versloot
 *
 */
public class Frame extends CVParticle {

	public final static String NO_IMAGE = "none";
	public final static String JPG_IMAGE = "jpg";
	public final static String PNG_IMAGE = "png";
	public final static String GIF_IMAGE = "gif";
	
	private long timeStamp;
	private String imageType = JPG_IMAGE;
	private byte[] imageBytes;
	private BufferedImage image;
	private Rectangle boundingBox;
	private List<Feature> features = new ArrayList<Feature>();
	
	public Frame(String streamId, long sequenceNr, String imageType, BufferedImage image, long timeStamp, Rectangle boundingBox, List<Feature> features) throws IOException {
		this(streamId, sequenceNr, imageType, image, timeStamp, boundingBox);
		if(features != null) this.features = features;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, BufferedImage image, long timeStamp, Rectangle boundingBox ) throws IOException {
		super( streamId, sequenceNr);
		this.imageType = imageType;
		setImage(image);
		this.timeStamp = timeStamp;
		this.boundingBox = boundingBox;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, byte[] image, long timeStamp, Rectangle boundingBox, List<Feature> features) {
		this(streamId, sequenceNr, imageType, image, timeStamp, boundingBox);
		if(features != null) this.features = features;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, byte[] image, long timeStamp, Rectangle boundingBox ) {
		super(streamId, sequenceNr);
		this.imageType = imageType;
		this.imageBytes = image;
		this.timeStamp = timeStamp;
		this.boundingBox = boundingBox;
	}
	
	public Frame(Tuple tuple, String imageType, byte[] image, long timeStamp, Rectangle box) {
		super(tuple);
		this.imageType = imageType;
		this.imageBytes = image;
		this.timeStamp = timeStamp;
		this.boundingBox = box;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public Rectangle getBoundingBox() {
		return boundingBox;
	}

	
	public BufferedImage getImage() throws IOException {
		if(imageBytes == null) {
			imageType = NO_IMAGE;
			return null;
		}
		if(image == null){
			image = ImageUtils.bytesToImage(imageBytes);
		}
		return image;
	}

	public void setImage(BufferedImage image) throws IOException {
		this.image = image;
		if(image != null){
			if(imageType.equals(NO_IMAGE)) imageType = JPG_IMAGE;
			this.imageBytes = ImageUtils.imageToBytes(image, imageType);
		}else{
			this.imageBytes = null;
			this.imageType = NO_IMAGE;
		}
	}
	
	public void setImage(byte[] imageBytes, String imgType){
		this.imageBytes = imageBytes;
		this.imageType = imgType;
		this.image = null;
	}
	
	public void removeImage(){
		this.image = null;
		this.imageBytes = null;
		this.imageType = NO_IMAGE;
	}


	public long getTimestamp(){
		return this.timeStamp;
	}

	public List<Feature> getFeatures() {
		return features;
	}

	public String getImageType() {
		return imageType;
	}

	public void setImageType(String imageType) throws IOException {
		this.imageType = imageType;
		if(image != null){
			imageBytes = ImageUtils.imageToBytes(image, imageType);
		}else{
			image = ImageUtils.bytesToImage(imageBytes);
			imageBytes = ImageUtils.imageToBytes(image, imageType);
		}
	}

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public String toString(){
		String result= "Frame : {streamId:"+getStreamId()+", sequenceNr:"+getSequenceNr()+", timestamp:"+getTimestamp()+", imageType:"+imageType+", features:[ ";
		for(Feature f : features) result += f.getName()+" = "+f.getSparseDescriptors().size()+", ";
		return result + "] }";
	}
}


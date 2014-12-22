package nl.tno.stormcv.util.connector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;

import nl.tno.stormcv.StormCVConfig;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * A {@link FileConnector} implementation used to access AWS S3 buckets. The S3_KEY and S3_SECRET to be used 
 * must be provided in the {@link StormCVConfig}. This connector is used for locations starting with <b>s3://</b>
 * 
 * @author Corne Versloot
 *
 */
public class S3Connector implements FileConnector {

	/**
	 * Configuration key used to set the AWS S3 Key in {@link StormCVConfig}
	 */
	public static final String S3_KEY = "stormcv.s3.key";
	
	/**
	 * Configuration key used to set the AWS S3 Secret in {@link StormCVConfig}
	 */
	public static final String S3_SECRET = "stormcv.s3.secret";
	
	private static final long serialVersionUID = 4829546208475679599L;
	
	private static final String PREFIX = "s3";
	private URI s3URI;
	private AmazonS3 s3;
	private String s3Key;
	private String s3Secret;
	private String[] extensions;
	private File tmpDir = null;
	
	public S3Connector(){};
	
	private S3Connector(String s3Key, String s3Secret, URI s3URI){
		this.s3Key = s3Key;
		this.s3Secret = s3Secret;
		this.s3 = new AmazonS3Client(new BasicAWSCredentials(s3Key, s3Secret));
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf) throws DatatypeConfigurationException{
		if(!stormConf.containsKey(S3_KEY)) throw new DatatypeConfigurationException("STORMCV_AWS_S3_KEY not set!"); 
		if(!stormConf.containsKey(S3_SECRET)) throw new DatatypeConfigurationException("STORMCV_AWS_S3_SECRET not set!");
		s3Key = (String)stormConf.get(S3_KEY);
		s3Secret = (String)stormConf.get(S3_SECRET);
		this.s3 = new AmazonS3Client(new BasicAWSCredentials(s3Key, s3Secret));
	}
	
	public S3Connector s3Key(String key){
		this.s3Key = key;
		return this;
	}
	
	public S3Connector s3Secret(String secret){
		this.s3Secret = secret;
		return this;
	}
	
	@Override
	public S3Connector setExtensions(String[] extensions) {
		this.extensions = extensions;
		return this;
	}

	@Override
	public void moveTo(String location) throws IOException{
		try {
			this.s3URI = new URI(location);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	@Override
	public List<String> list() {
		List<String> files = new ArrayList<String>();
		if(s3URI == null) return files;
		String[] buckAndKey = this.getBucketAndKey();
		String bucket = buckAndKey[0];
		String key = buckAndKey[1];
		ObjectListing objectListing = s3. listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(key));
		s3list: for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			// check for valid extensions
			if(extensions == null){
				files.add("s3://"+bucket+"/"+objectSummary.getKey());
			} else for(String ext : extensions) if(objectSummary.getKey().endsWith(ext)){
				files.add("s3://"+bucket+"/"+objectSummary.getKey());
				continue s3list;
			}
		 }
		return files;
	}

	@Override
	public String getProtocol() {
		return PREFIX;
	}

	@Override
	public File getAsFile() throws IOException{
		if(s3URI == null) throw new FileNotFoundException("No location set, use moveTo first!");
		if(tmpDir == null) {
			tmpDir = File.createTempFile("abcde", "tmp").getParentFile();
		}
		String[] bucketKey = getBucketAndKey();
		String bucket = bucketKey[0];
		String key = bucketKey[1];
		File tmpFile = new File(tmpDir, key.substring(key.lastIndexOf("/")+1));
		tmpFile.deleteOnExit();
		s3.getObject(new GetObjectRequest(bucket, key), tmpFile);
		return tmpFile;
	}

	@Override
	public void copyFile(File localFile, boolean delete) throws IOException {
		if(s3URI == null) throw new FileNotFoundException("No location set, use moveTo first!");
		String[] bucketKey = getBucketAndKey();
		s3.putObject(new PutObjectRequest(bucketKey[0], bucketKey[1], localFile));
		if(delete) localFile.delete();
	}

	
	private String[] getBucketAndKey(){
		String[] bucketKey = new String[2];
		bucketKey[0] = s3URI.getHost();
		bucketKey[1] = s3URI.getPath();
		if(bucketKey[1].startsWith("/")) bucketKey[1] = bucketKey[1].substring(1);
		return bucketKey;
		/*
		String s3Url = s3URL.substring(5);
		String bucket = s3Url.substring(0, s3Url.indexOf('/'));
		String key = s3Url.substring(s3Url.indexOf('/')+1);
		return new String[]{bucket, key};
		*/
	}

	@Override
	public FileConnector deepCopy() {
		return new S3Connector(s3Key, s3Secret, s3URI);
	}

}

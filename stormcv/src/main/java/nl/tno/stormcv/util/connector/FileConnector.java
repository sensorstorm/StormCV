package nl.tno.stormcv.util.connector;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;

import nl.tno.stormcv.StormCVConfig;

/**
 * An interface for connectors to (remote) locations like AWS S3. Implementing this interface and registering 
 * the implementation in the {@link StormCVConfig} will enable the platform to access the remote location.
 * 
 * @author Corne Versloot
 *
 */

public interface FileConnector extends Serializable{

	@SuppressWarnings("rawtypes")
	/**
	 * Prepare can be called to get storm configuration in FileConector (typically performed by 
	 * {@link Operation}s/{@link Fetcher}s that receive StormConf and require FileConnector;
	 * @param stormConf
	 */
	public void prepare(Map stormConf) throws DatatypeConfigurationException;
	
	/**
	 * Specify the valid extensions to use by this FileLocation (used by list).
	 * If no extensions are set all files will match 
	 * @param extensions
	 * @return itself
	 */
	public FileConnector setExtensions(String[] extensions);
	
	/**
	 * Moves the remote location to the specified point 
	 * @param location
	 * @return
	 */
	public void moveTo(String location) throws IOException;
	
	/**
	 * Copies the specified local file to the currently set (remote) location. 
	 * If the remote location is a 'directory' it will copy the file into the directory.
	 * If the remote location points to a file the remote file will likely be overwritten 
	 * (or an exception is thrown).
	 * @param localFile
	 * @param delete indicates if the localFile must be deleted after a successful copy
	 * @throws IOException
	 */
	public void copyFile(File localFile, boolean delete) throws IOException;
	
	/**
	 * List all 'files' within the current location which match the provided extensions.
	 * If the extensions is null than all files will be returned.
	 * @return
	 */
	public List<String> list();
	
	/**
	 * Returns the FileLocation's prefix (ftp, s3, file, ...)
	 * @return
	 */
	public String getProtocol();
	
	/**
	 * Gets the current location as a file. If the location is remote this will trigger a download
	 * @return
	 */
	public File getAsFile() throws IOException;
	 
	/**
	 * Makes a deep copy of this object 
	 * @return
	 */
	public FileConnector deepCopy();
	
}
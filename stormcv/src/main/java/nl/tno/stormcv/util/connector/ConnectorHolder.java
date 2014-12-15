package nl.tno.stormcv.util.connector;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.StormCVConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class used to maintain a set of {@link FileConnector} implementations registered in the {@link StormCVConfig}.
 * An instantiated ConnectorHolder can be fetched from the Storm configuration after which it can be used to request
 * connectors for urls. getConnector("s3://bucket/path/bideo.mp4") will return the configured {@link S3Connector} object.  
 * 
 * @author Corne Versloot
 *
 */
public class ConnectorHolder {

	private HashMap<String, FileConnector> connectors;
	Logger logger = LoggerFactory.getLogger(ConnectorHolder.class);
	
	/**
	 * Uses the provided stormConf to instantiate and configure registered {@link FileConnector}'s
	 * using {@link StormCVConfig}.STORMCV_CONNECTORS
	 * @param stormConf
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ConnectorHolder(Map stormConf){
		connectors = new HashMap<String, FileConnector>();
		List<String> registered = (List<String>)stormConf.get(StormCVConfig.STORMCV_CONNECTORS);
		for(String name : registered) try{
			FileConnector connector = (FileConnector)Class.forName(name).newInstance();
			connector.prepare(stormConf);
			connectors.put(connector.getProtocol(), connector);
		}catch(Exception e){
			logger.warn("Unable to instantiate FileConnector: "+name+" due to: "+e.getMessage());
		}
	}
	
	public FileConnector getConnector(String location){
		try {
			return connectors.get(new URI(location).getScheme());
		} catch (URISyntaxException e) {
			logger.warn(location+" does hot have valid URI syntax: "+e.getMessage());
		}
		return null;
	}
	
}

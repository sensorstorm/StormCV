package nl.tno.stormcv.fetcher;

import java.io.Serializable;
import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.spout.CVParticleSpout;
import backtype.storm.task.TopologyContext;

/**
 * Fetcher implementations are executed within {@link CVParticleSpout} and are responsible to read data and push it into topologies.
 * Fetcher's produce a {@link CVParticle} implementation which has its own {@link CVParticleSerializer} implementation as well. 
 * 
 * @author Corne Versloot
 */

public interface IFetcher<Output extends CVParticle> extends Serializable {

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) throws Exception;
	
	public CVParticleSerializer<Output> getSerializer();
	
	public void activate();

	public void deactivate() ;
	
	public Output fetchData();
	
}

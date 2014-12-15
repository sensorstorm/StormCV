package nl.tno.stormcv.spout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.fetcher.IFetcher;
import nl.tno.stormcv.model.CVParticle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

/**
 * Basic spout implementation that includes fault tolerance (if activated). It should be noted that running the spout in fault 
 * tolerant mode will use more memory because emitted tuples are cashed for a limited amount of time (which is configurable).
 * 
 * THe actual reading of data is done by {@link IFetcher} implementations creating {@link CVParticle} objects which are serialized
 * and emitted by this spout.
 *  
 * @author Corne Versloot
 */
public class CVParticleSpout implements IRichSpout{
	
	private static final long serialVersionUID = 2828206148753936815L;
	
	private Logger logger = LoggerFactory.getLogger(CVParticleSpout.class);
	private Cache<Object, Object> tupleCache; // a cache holding emitted tuples so they can be replayed on failure
	protected SpoutOutputCollector collector;
	private boolean faultTolerant = false;
	private IFetcher<? extends CVParticle> fetcher;
	
	public CVParticleSpout(IFetcher<? extends CVParticle> fetcher){
		this.fetcher = fetcher;
	}
	
	/**
	 * Indicates if this Spout must cache tuples it has emitted so they can be replayed on failure.
	 * This setting does not effect anchoring of tuples (which is always done to support TOPOLOGY_MAX_SPOUT_PENDING configuration) 
	 * @param faultTolerant
	 * @return
	 */
	public CVParticleSpout setFaultTolerant(boolean faultTolerant){
		this.faultTolerant = faultTolerant;
		return this;
	}
	
	/**
	 * Configures the spout by fetching optional parameters from the provided configuration. If faultTolerant is true the open
	 * function will also construct the cache to hold the emitted tuples.
	 * Configuration options are:
	 * <ul>
	 * <li>stormcv.faulttolerant --> boolean: indicates if the spout must operate in fault tolerant mode (i.e. replay tuples after failure)</li>
	 * <li>stormcv.tuplecache.timeout --> long: timeout (seconds) for tuples in the cache </li>
	 * <li>stormcv.tuplecache.maxsize --> int: maximum number of tuples in the cache (used to avoid memory overload)</li>
	 * </ul>
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		if(conf.containsKey(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT)){
			faultTolerant = (Boolean) conf.get(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT);
		}
		if(faultTolerant){
			long timeout = conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ? 30 : (Long)conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC);
			int maxSize = conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE) == null ? 500 : ((Long)conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE)).intValue();
			tupleCache = CacheBuilder.newBuilder()
					.maximumSize(maxSize)
					.expireAfterAccess(timeout, TimeUnit.SECONDS)
					.build();
		}

		// pass configuration to subclasses
		try {
			fetcher.prepare(conf, context);
		} catch (Exception e) {
			logger.warn("Unable to configure spout due to ", e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fetcher.getSerializer().getFields());
	}
	
	@Override
	public void nextTuple(){
		CVParticle particle = fetcher.fetchData();
		
		if(particle != null) try {
			Values values = fetcher.getSerializer().toTuple(particle);
			String id = particle.getStreamId()+"_"+particle.getSequenceNr();
			if(faultTolerant && tupleCache != null) tupleCache.put(id, values);
			collector.emit(values, id);
		} catch (IOException e) {
			logger.warn("Unable to fetch next frame from queue due to: "+e.getMessage());
		}
	}
	
	@Override
	public void close() {
		if(faultTolerant && tupleCache != null){
			tupleCache.cleanUp();
		}
		fetcher.deactivate();
	}

	@Override
	public void activate() {
		fetcher.activate();
	}

	@Override
	public void deactivate() {
		fetcher.deactivate();
	}

	@Override
	public void ack(Object msgId) {
		if(faultTolerant && tupleCache != null){
			tupleCache.invalidate(msgId);
		}
	}

	@Override
	public void fail(Object msgId) {
		logger.debug("Fail of: "+msgId);
		if(faultTolerant && tupleCache != null && tupleCache.getIfPresent(msgId) != null){
			collector.emit((Values)tupleCache.getIfPresent(msgId), msgId);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
		
}

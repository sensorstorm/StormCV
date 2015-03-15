package nl.tno.stormcv.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.PersistentArrayMap;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * StormCV's basic BaseRichBolt implementation that supports the use of {@link CVParticle} objects.
 * This bolt supports fault tolerance if it is configured to do so and supports the serialization of model objects.
 * 
 * @author Corne Versloot
 *
 */
public abstract class CVParticleBolt extends BaseRichBolt{

	private static final long serialVersionUID = -5421951488628303992L;
	
	protected Logger logger = LoggerFactory.getLogger(CVParticleBolt.class);
	protected HashMap<String, CVParticleSerializer<? extends CVParticle>> serializers = new HashMap<String, CVParticleSerializer<? extends CVParticle>>();
	protected OutputCollector collector;
	protected String boltName;
	protected long idleTimestamp = -1;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.boltName = context.getThisComponentId();
		
		try{
			PersistentArrayMap map = (PersistentArrayMap)conf.get(Config.TOPOLOGY_KRYO_REGISTER);
			for(Object className : map.keySet()){
				serializers.put((String)className, (CVParticleSerializer<? extends CVParticle>)Class.forName((String)map.get(className)).newInstance());
			}
		}catch(Exception e){
			logger.error("Unable to prepare CVParticleBolt due to ",e);
		}
		
		this.prepare(conf, context);
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void execute(Tuple input) {
		try{
			CVParticle cvt = deserialize(input);
			List<? extends CVParticle> results = execute(cvt);
			for(CVParticle output : results){
				output.setRequestId(cvt.getRequestId());
				CVParticleSerializer serializer = serializers.get(output.getClass().getName());
				if(serializers.containsKey(output.getClass().getName())){
					collector.emit(input, serializer.toTuple(output));
				}else{
					// TODO: what else?
				}
			}
			collector.ack(input);
		}catch(Exception e){
			logger.warn("Unable to process input", e);
			collector.fail(input);
		}
		idleTimestamp = System.currentTimeMillis();
	}
	
	/**
	 * Deserializes a Tuple into a CVParticle type
	 * @param tuple
	 * @return
	 * @throws IOException 
	 */
	protected CVParticle deserialize(Tuple tuple) throws IOException{
		String typeName = tuple.getStringByField(CVParticleSerializer.TYPE);
		return serializers.get(typeName).fromTuple(tuple);
	}
	
	/**
	 * Subclasses must implement this method which is responsible for analysis of 
	 * received CVParticle objects. A single input object may result in zero or more
	 * resulting objects which will be serialized and emitted by this Bolt.
	 * 
	 * @param input
	 * @return
	 */
	abstract List<? extends CVParticle> execute(CVParticle input) throws Exception;
	
	@SuppressWarnings("rawtypes")
	abstract void prepare(Map stormConf, TopologyContext context);
}

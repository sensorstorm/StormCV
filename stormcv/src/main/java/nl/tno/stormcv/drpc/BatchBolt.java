package nl.tno.stormcv.drpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.PersistentArrayMap;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchBolt extends BaseBatchBolt<Long>{

	private static final long serialVersionUID = 1280873294403747903L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private IBatchOp<? extends CVParticle> batchOp;
	private IResultOp resultOp;
	private long requestId;

	private HashMap<String, CVParticleSerializer<? extends CVParticle>> serializers;
	private BatchOutputCollector collector;

	public BatchBolt(IBatchOp<? extends CVParticle> operation) {
		this.batchOp = operation;
	}
	
	public BatchBolt(IResultOp operation) {
		this.resultOp = operation;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Long requestId) {
		this.collector = collector;
		this.requestId = requestId;
		try{
			if(serializers == null){	
				serializers = new HashMap<String, CVParticleSerializer<? extends CVParticle>>();
				PersistentArrayMap map = (PersistentArrayMap)conf.get(Config.TOPOLOGY_KRYO_REGISTER);
				for(Object className : map.keySet()){
					serializers.put((String)className, (CVParticleSerializer<? extends CVParticle>)Class.forName((String)map.get(className)).newInstance());
				}
			}
			if(batchOp != null) batchOp.initBatch(conf, context);
			if(resultOp != null) resultOp.initBatch(conf, context);
		}catch(Exception e){
			logger.error("Unable to prepare CVParticleBolt due to ",e);
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		CVParticle particle;
		try {
			particle = deserialize(tuple);
			if(batchOp != null) batchOp.processData(particle);
			if(resultOp != null) resultOp.processData(particle);
		} catch ( IOException e) {
			logger.error("Unalbe deserialize tuple: "+tuple.getStringByField(CVParticleSerializer.TYPE));
		} catch (Exception e) {
			logger.error("Unable to process data for request: "+tuple.getLongByField(CVParticleSerializer.REQUESTID));
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void finishBatch() {
		try{
			if(resultOp != null){
				String result = resultOp.getFinalResult();
				collector.emit(new Values(requestId, result));
			}else if(batchOp != null){
				List<? extends CVParticle> results = batchOp.getBatchResult();
				for(CVParticle output : results){
					CVParticleSerializer serializer = serializers.get(output.getClass().getName());
					if(serializers.containsKey(output.getClass().getName())){
						collector.emit(serializer.toTuple(output));
					}else{
						// use serializer from operation as fall back
						collector.emit(batchOp.getSerializer().toTuple(output));
					}
				}
			}
		}catch(Exception e){
			logger.error("Unable to finish batch!");
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(batchOp != null){
			declarer.declare(batchOp.getSerializer().getFields());
		}else{
			declarer.declare(new Fields("id", "result"));
		}
	}
	
	/**
	 * Deserializes a Tuple into a CVParticle type
	 * @param tuple
	 * @return
	 * @throws IOException 
	 */
	private CVParticle deserialize(Tuple tuple) throws IOException{
		String typeName = tuple.getStringByField(CVParticleSerializer.TYPE);
		return serializers.get(typeName).fromTuple(tuple);
	}

}

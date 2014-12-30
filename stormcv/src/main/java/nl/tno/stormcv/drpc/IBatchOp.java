package nl.tno.stormcv.drpc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.IOperation;
import backtype.storm.task.TopologyContext;

public interface IBatchOp <Output extends CVParticle> extends Serializable{

	/**
	 * Called each time a new request has to be answered
	 * @param stormConf
	 * @param context
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public void initBatch(Map stormConf, TopologyContext context) throws Exception;
	
	/**
	 * Called every time there is data to be added to the current result
	 * @param particle
	 * @throws Exception
	 */
	public void processData(CVParticle particle) throws Exception;
	
	/**
	 * Called when all data for a batch has been processed
	 * @return
	 * @throws Exception
	 */
	public List<Output> getBatchResult() throws Exception;
	
	/**
	 * Provides the {@link CVParticleSerializer} implementation to be used to serialize the output of the {@link IOperation}
	 * @return
	 */
	public CVParticleSerializer<Output> getSerializer();

}

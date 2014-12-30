package nl.tno.stormcv.drpc;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;

public interface IResultOp extends Serializable{

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
	public String getFinalResult() throws Exception;
	
}
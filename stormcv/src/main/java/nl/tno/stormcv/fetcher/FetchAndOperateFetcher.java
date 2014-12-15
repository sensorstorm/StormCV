package nl.tno.stormcv.fetcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.IBatchOperation;
import nl.tno.stormcv.operation.IOperation;
import nl.tno.stormcv.operation.SequentialFrameOp;
import nl.tno.stormcv.operation.ISingleInputOperation;

/**
 * A meta {@link IFetcher} that enables the use of an Operation directly within the spout. Data fetched is directly executed by the 
 * configured operation. This avoids data transfer from Spout to Bolt but is only useful when the operation can keep up with the 
 * load provided by the fetcher. A typical setup will be some Fetcher emitting Frame objects and a {@link ISingleInputOperation}  
 * that processes the Frame. Hence it is also possible to use a {@link SequentialFrameOp}.
 * 
 * It is possible to provide a {@link IBatchOperation} if the output of the Fetcher is set to emit {@link GroupOfFrames} using the
 * groupOfFramesOutput(...)  function some Fetchers have.
 * 
 * @author Corne Versloot
 *
 */
@SuppressWarnings("rawtypes")
public class FetchAndOperateFetcher implements IFetcher<CVParticle> {

	private static final long serialVersionUID = -1900017732079975170L;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private IFetcher fetcher;
	private boolean sio = false;
	private ISingleInputOperation singleInOp;
	private IBatchOperation batchOp;
	private List<CVParticle> results;

	/**
	 * Instantiate an {@link FetchAndOperateFetcher} by providing the Fetcher an Operation to be used.
	 * The operation can be either a SingleInputOpearion or a BatchOperation. BatchOperation's can only
	 * be used if the fetcher is configured to output GroupOfFrame objects instead of Frame's. 
	 * @param fetcher
	 * @param operation
	 */
	public FetchAndOperateFetcher(IFetcher  fetcher, IOperation  operation) {
		this.fetcher = fetcher;
		if(operation instanceof ISingleInputOperation){
			singleInOp = (ISingleInputOperation) operation;
			sio = true;
		}else{
			batchOp = (IBatchOperation) operation;
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context)	throws Exception {
		fetcher.prepare(stormConf, context);
		if(sio) singleInOp.prepare(stormConf, context);
		else batchOp.prepare(stormConf, context);
		results = new ArrayList<CVParticle>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		if(sio) return singleInOp.getSerializer();
		return batchOp.getSerializer();
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
	@SuppressWarnings("unchecked")
	public CVParticle fetchData() {
		// first empty the current list with particles before fetching new data
		if(results.size() > 0) return results.remove(0);
		
		// fetch data from particle
		CVParticle particle = fetcher.fetchData();
		if(particle == null) return null;
		try{
			if(sio) results = singleInOp.execute(particle);
			else if (!sio && particle instanceof GroupOfFrames){
				List<CVParticle> particles = new ArrayList<CVParticle>();
				particles.addAll( ((GroupOfFrames)particle).getFrames() );
				results = batchOp.execute(particles);
			}else{
				logger.warn("BatchOperation configured did not get a GroupOfFrames object from from the Fetcher, got "+particle.getClass().getName()+" instead" );
			}
		}catch(Exception e){
			logger.error("Unable to process fetched data due to: "+e.getMessage(), e);
		}
		if(results.size() > 0) return results.remove(0);
		return null;
	}

}

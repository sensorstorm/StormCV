package nl.tno.stormcv.bolt;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.IBatcher;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IBatchOperation;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * A {@link CVParticleBolt} that stores all input in the History until some criteria are met. Items will remain in the History for a 
 * predefined period ( {@link StormCVConfig}.STORMCV_CACHES_TIMEOUT_SEC) after which they will be removed and failed automatically. 
 * The BatchInputBolt works like follows:
 * <ol>
 * <li>The execute(Tuple) function serializes the Tuple to a {@link CVParticle} object</li>
 * <li>This object is added to the History based on the specified Group (i.e. set of fields, similar to fieldsGrouping used within storm).
 * Items in a group are ordered ASC based on their sequenceNr. If refreshExpirationOfOlderItems is true any existing items in the group with 
 * higher sequenceNr than the new one will have their expiration timer reset.</li>
 * <li>The provided {@link IBatcher} is called on the grouped items the new item was added to. The Batcher is responsible for the
 * creation of appropriate batches which typically depend on the {@link IBatchOperation} implementation provided on construction. The Batcher may return
 * zero or more batches and is responsible for the removal of items from the History that are no longer needed which will result in an 
 * ACK on that tuple. Not doing so will result in expiration of items from the cache which in turn will cause failures of tuples.</li>
 * <li>The BatchOperation is called for each batch and the results are emitted by the bolt</li>
 * </ol>
 * <p>
 * An example setup might be to have a window Batcher that waits until 10 input items have been received after which 
 * a BatchOperation calculates some average value for it. The Batcher will remove the 10 items from the history and
 * will wait for the next 10 etc.
 * 
 * <p/><b>Important: </b> due to the parallelism it is very unlikely that items will be received in their original order which can may result in
 * the scenario that items are evicted from the cache before the Batcher could add them to a valid partition. Setting 
 * refreshExpirationOfOlderItems to true will <i>minimize</i> this Batcher by resetting the timer on all items in the history of a group when a new 
 * item with lower sequenceNr is received. This will however use more memory and does not solve the underlying problem of having a 'Bolt that is to slow' 
 * somewhere in the preceding topology. Using higher parallelism hints or setting setMaxSpoutPending() for the topology can solve the problem. 
 * 
 * <p><b><Configuration:</b><br/>
 * Parameters set through storm configuration:
 * <ul>
 * <li>StromCVConfig.TOPOLOGY_CACHES_TIMEOUT_SEC (Integer): sets the timeout in seconds for items cashed by the History. The default is 20 seconds</li>
 * <li>StromCVConfig.TOPOLOGY_CACHES_MAXSIZE (Integer): sets the maximum size of the History cache (to avoid memory overload). The default is 252</li>
 * </ul>
 * 
 * @author Corne Versloot
 *
 */
public class BatchInputBolt extends CVParticleBolt implements RemovalListener<CVParticle, String>{

	private static final long serialVersionUID = -2394218774274388493L;

	private IBatchOperation<? extends CVParticle> operation;
	private IBatcher batcher;
	private int TTL = 29;
	private int maxSize = 256;
	private Fields groupBy;
	private History history;
	private boolean refreshExperation = true;
	
	/**
	 * Creates a BatchInputBolt with given Batcher and BatchOperation.
	 * @param batcher
	 * @param operation
	 * @param refreshExpirationOfOlderItems
	 */
	public BatchInputBolt(IBatcher batcher, IBatchOperation<? extends CVParticle> operation){
		this.operation = operation;
		this.batcher = batcher;
	}
	
	/**
	 * Sets the time to live for items being cached by this bolt
	 * @param ttl
	 * @return
	 */
	public BatchInputBolt ttl(int ttl){
		this.TTL = ttl;
		return this;
	}
	
	/**
	 * Specifies the maximum size of the cashe used. Hitting the maximum will cause oldest items to be 
	 * expired from the cache
	 * @param size
	 * @return
	 */
	public BatchInputBolt maxCacheSize(int size){
		this.maxSize = size;
		return this;
	}
	
	/**
	 * Specifies the fields used to group items on. 
	 * @param group
	 * @return
	 */
	public BatchInputBolt groupBy(Fields group){
		this.groupBy = group;
		return this;
	}
	
	/**
	 * Specifies weather items with hither sequence number within a group must have their
	 * ttl's refreshed if an item with lower sequence number is added
	 * @param refresh
	 * @return
	 */
	public BatchInputBolt refreshExpiration(boolean refresh){
		this.refreshExperation = refresh;
		return this;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	void prepare(Map conf, TopologyContext context) {
		// use TTL and maxSize from config if they were not set explicitly using the constructor (implicit way of doing this...)
		if(TTL == 29) TTL = conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ? TTL : ((Long)conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC)).intValue();
		if(maxSize == 256) maxSize = conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE) == null ? maxSize : ((Long)conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE)).intValue();
		history = new History(this);
		
		// IF NO grouping was set THEN select the first grouping registered for the spout as the grouping used within the Spout (usually a good guess)
		if(groupBy == null){
			Map<GlobalStreamId, Grouping> sources = context.getSources(context.getThisComponentId());
			for(GlobalStreamId id : sources.keySet()){
				Grouping grouping = sources.get(id);
				this.groupBy = new Fields(grouping.get_fields());
				break;
			}
		}
		
		// prepare the selector and operation
		try {
			batcher.prepare(conf);
			operation.prepare(conf, context);
		} catch (Exception e) {
			logger.error("Unable to preapre the Selector or Operation", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(operation.getSerializer().getFields());
	}

	/**
	 * Overloaded from superclass, adds the input to the history and asks the selector to create batches (if possible)
	 * for all the items in the history. If one or multiple batches could be created the operation
	 * will be executed for each batch and the results are emitted by this bolt.
	 */
	@Override
	public void execute(Tuple input) {
		String group = generateKey(input);
		if(group == null){
			collector.fail(input);
			return;
		}
		CVParticle particle;
		try {
			particle = deserialize(input);
			history.add(group, particle);
			List<List<CVParticle>> batches = batcher.partition(history, history.getGroupedItems(group));
			for(List<CVParticle> batch : batches){
				try{
					List<? extends CVParticle> results = operation.execute(batch);
					for(CVParticle result : results){
						result.setRequestId(particle.getRequestId());
						collector.emit(input, serializers.get(result.getClass().getName()).toTuple(result));
					}
				}catch(Exception e){
					logger.warn("Unable to to process batch due to ", e);
				}
			}
		} catch (IOException e1) {
			logger.warn("Unable to deserialize Tuple", e1);
		}
		idleTimestamp = System.currentTimeMillis();
	}
	
	@Override
	List<? extends CVParticle> execute(CVParticle input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Generates the key for the provided tuple using the fields provided at construction time
	 * @param tuple
	 * @return key created for this tuple or NULL if no key could be created (i.e. tuple does not contain any of groupBy Fields)
	 */
	private String generateKey(Tuple tuple){
		String key = new String();
		for(String field : groupBy){
			key += tuple.getValueByField(field)+"_";
		}
		if(key.length() == 0) return null;
		return key;
	}
	
	/**
	 * Callback method for removal of items from the histories cache. Items removed from the cache need to be acked or failed
	 * according to the reason they were removed
	 */
	@Override
	public void onRemoval(RemovalNotification<CVParticle, String> notification) {
		// make sure the CVParticle object is removed from the history (even if removal was automatic!)
		history.clear(notification.getKey(), notification.getValue());
		if(notification.getCause() == RemovalCause.EXPIRED || notification.getCause() == RemovalCause.SIZE){
			// item removed automatically --> fail the tuple
			collector.fail(notification.getKey().getTuple());
		}else{
			// item removed explicitly --> ack the tuple
			collector.ack(notification.getKey().getTuple());
		}
	}
	
	// ----------------------------- HISTORY CONTAINER ---------------------------
	/**
	 * Container that manages the history of tuples received and allows others to clean up the history retained.
	 *  
	 * @author Corne Versloot
	 *
	 */
	public class History implements Serializable{
		
		private static final long serialVersionUID = 5353548571380002710L;
		
		private Cache<CVParticle, String> inputCache;
		private HashMap<String, List<CVParticle>> groups;
		
		/**
		 * Creates a History object for the specified Bolt (which is used to ack or fail items removed from the history).
		 * @param bolt
		 */
		private History(BatchInputBolt bolt){
			groups = new HashMap<String, List<CVParticle>>();
			inputCache = CacheBuilder.newBuilder()
					.maximumSize(maxSize)
					.expireAfterAccess(TTL, TimeUnit.SECONDS) // resets also on get(...)!
					.removalListener(bolt)
					.build();
		}
		
		/**
		 * Adds the new CVParticle object to the history and returns the list of items it was grouped with. 
		 * @param group the name of the group the CVParticle belongs to
		 * @param particle the CVParticle object that needs to be added to the history.
		 */
		private void add(String group, CVParticle particle){
			if(!groups.containsKey(group)){
				groups.put(group, new ArrayList<CVParticle>());
			}
			List<CVParticle> list = groups.get(group);
			int i;
			for(i = list.size()-1; i>=0; i--){
				if( particle.getSequenceNr() > list.get(i).getSequenceNr()){
					list.add(i+1, particle);
					break;
				}
				if(refreshExperation){
					inputCache.getIfPresent(list.get(i)); // touch the item passed in the cache to reset its expiration timer
				}
			}
			if(i < 0) list.add(0, particle);
			inputCache.put(particle, group);
		}
		
		/**
		 * Removes the object from the history. This will tricker an ACK to be send. 
		 * @param particle
		 */
		public void removeFromHistory(CVParticle particle){
			inputCache.invalidate(particle);
		}
		
		/**
		 * Removes the object from the group
		 * @param particle
		 * @param group
		 */
		private void clear(CVParticle particle, String group){
			if(!groups.containsKey(group)) return;
			groups.get(group).remove(particle);
			if(groups.get(group).size() == 0) groups.remove(group);
		}
		
		/**
		 * Returns all the items in this history that belong to the specified group
		 * @param group
		 * @return
		 */
		public List<CVParticle> getGroupedItems(String group){
			return groups.get(group); 
		}
		
		public long size(){
			return inputCache.size();
		}
		
		public String toString(){
			String result = "";
			for(String group : groups.keySet()){
				result += "  "+group+" : "+groups.get(group).size()+"\r\n";
			}
			return result;
		}
	}// end of History class

}

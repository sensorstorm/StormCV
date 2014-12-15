package nl.tno.stormcv.grouping;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

/**
 * A custom grouping that functions as a <b>filter</b> as well as (optionally) a FieldGrouping. A mod value is provided during construction and is used
 * to filter {@link CVParticle} objects. Only objects which sequenceNR % mod == 0 will be passed through the filter. It is possible to supplier a set of Fields
 * which will define what task the accepted tuple will be assigned to (similar to fieldGrouping). If no fields are provided the task is
 * chosen randomly (like shuffleGrouping). 
 * 
 * @author Corne Versloot
 */
public class ModGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -4499075514313889316L;

	public int mod;
	public int sequenceIndex = -1;
	private List<Integer> targetTasks;
	private List<Integer> indexes;
	
	/**
	 * @param mod only tuples with sequenceNr % mod == 0 will be accepted
	 * @param serializer the serializer used to interpret tuples provided
	 * @param grouping the set of fields used to choose the taks
	 * @throws InstantiationException
	 */
	@SuppressWarnings("rawtypes")
	public ModGrouping(int mod, CVParticleSerializer serializer, Fields grouping) throws InstantiationException{
		this.mod = mod;
		Fields fields = serializer.getFields();
		indexes = new ArrayList<Integer>();
		for(String groupBy : grouping){
			for(int i=0; i<fields.size(); i++){
				if(groupBy.equals(fields.get(i))){
					indexes.add(i);
				}
				if(fields.get(i).equals(CVParticleSerializer.SEQUENCENR)){
					sequenceIndex = i;
				}
			}
		}
		if(sequenceIndex == -1) throw new InstantiationException("No index found for Field '"+CVParticleSerializer.SEQUENCENR+"'");
		if(indexes.size() == 0) throw new InstantiationException("No field indexes found for provided grouping: "+grouping);
	}
	
	/**
	 * @param mod only tuples with sequenceNr % mod == 0 will be accepted
	 * @param serializer the serializer used to interpret tuples provided
	 * @throws InstantiationException
	 */
	@SuppressWarnings("rawtypes")
	public ModGrouping(int mod, CVParticleSerializer serializer) throws InstantiationException{
		this.mod = mod;
		Fields fields = serializer.getFields();
		for(int i=0; i<fields.size(); i++){
			if(fields.get(i).equals(CVParticleSerializer.SEQUENCENR)){
				sequenceIndex = i;
			}
		}
		
		if(sequenceIndex == -1) throw new InstantiationException("No index found for Field '"+CVParticleSerializer.SEQUENCENR+"'");
	}
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> targets = new ArrayList<Integer>();
		if((Long)values.get(sequenceIndex) % mod != 0 ) return targets;
		
		if(indexes != null && indexes.size() > 0){
			int hash = 0;
			for(int i : indexes){
				hash += values.get(i).hashCode();
			}
			targets.add(targetTasks.get(hash % targetTasks.size()));
		}else{
			int randI = (int)Math.floor(Math.random() * targetTasks.size());
			targets.add(targetTasks.get(randI));
		}
		return targets;
	}
	
}

package nl.tno.stormcv.drpc;


import java.util.List;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IOperation;

public interface IRequestOp <Output extends CVParticle> extends IOperation<Output>{

	public List<Output> execute(long requestId, String args) throws Exception;
}

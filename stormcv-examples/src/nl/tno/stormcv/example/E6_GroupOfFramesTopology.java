package nl.tno.stormcv.example;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.DiscreteWindowBatcher;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.example.util.OpticalFlowVisualizeOp;
import nl.tno.stormcv.fetcher.FileFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.FrameGrouperOp;
import nl.tno.stormcv.operation.GroupOfFramesOp;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.OpticalFlowOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.spout.CVParticleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class E6_GroupOfFramesTopology {

	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();

		/**
		 * Sets the OpenCV library to be used which depends on the system the topology is being executed on
		 */
		//conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java2411.dylib");

		conf.setNumWorkers(6); // number of workers in the topology
		conf.setMaxSpoutPending(32); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
		List<String> urls = new ArrayList<String>();
		urls.add( "file://"+ userDir +"/resources/data/The_Nut_Job_trailer.mp4" );

		int frameSkip = 13;
		
		TopologyBuilder builder = new TopologyBuilder();
		
		// spout reading streams emitting 2 frames group of frames (containing 2 frames) to the optical flow operation
		//builder.setSpout("spout", new CVParticleSpout( new StreamFrameFetcher(urls).frameSkip(frameSkip).groupSize(2) ), 1 ).setNumTasks(1);
		builder.setSpout("spout", new CVParticleSpout( new FileFrameFetcher(urls).frameSkip(frameSkip).groupSize(2) ), 1 ).setNumTasks(1);
		
		// add bolt that scales frames down to 50% of the original size 
		builder.setBolt("scale", new SingleInputBolt( new ScaleImageOp(0.5f)), 1)
			.shuffleGrouping("spout");
		
		builder.setBolt("grouper", new BatchInputBolt(new DiscreteWindowBatcher(2, 1), new FrameGrouperOp()), 1)
			.fieldsGrouping("scale",new Fields(FrameSerializer.STREAMID));
		
		// two group of frames operation calculating Optical Flow on Groups it receives. This makes the optical flow stateless! 
		builder.setBolt("optical_flow", new SingleInputBolt(
					new GroupOfFramesOp(new OpticalFlowOp("optical flow"))
				), 2).shuffleGrouping("grouper");
		
		// simple operation that visualizes the optical flow and puts it into a Frame object that can be shown / streamed
		builder.setBolt("flow_viz", new SingleInputBolt(new OpticalFlowVisualizeOp("optical flow")), 1
				).shuffleGrouping("optical_flow");
		
		// add bolt that creates a webservice on port 8558 enabling users to view the result
		builder.setBolt("streamer", new BatchInputBolt(
				new SlidingWindowBatcher(2, frameSkip).maxSize(6), 
				new MjpegStreamingOp().port(8558).framerate(5)).groupBy(new Fields(FrameSerializer.STREAMID))
			, 1)
			.shuffleGrouping("flow_viz");
		
		// NOTE: if the topology is started (locally) go to http://localhost:8558/streaming/tiles and click the image to see the stream!
		
		try {
			
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "bigflow", conf, builder.createTopology() );
			Utils.sleep(120*1000); // run for one minute and then kill the topology
			cluster.shutdown();
			System.exit(1);
			
			// run on a storm cluster
			// StormSubmitter.submitTopology("some_topology_name", conf, builder.createTopology());
		} catch (Exception e){
			e.printStackTrace();
		}
	}

}

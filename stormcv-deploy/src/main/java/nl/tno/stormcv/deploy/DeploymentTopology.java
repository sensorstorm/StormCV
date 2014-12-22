package nl.tno.stormcv.deploy;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.StreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.FeatureExtractionOp;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.operation.SequentialFrameOp;
import nl.tno.stormcv.spout.CVParticleSpout;

import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DeploymentTopology {

public static void main(String[] args){
		
		// first some global (topology) configuration
		StormCVConfig conf = new StormCVConfig();
		
		/**
		 * Sets the OpenCV library to be used which depends on the system the topology is being executed on
		 */
		//conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib");
		
		conf.setNumWorkers(4); // number of workers in the topology
		conf.setMaxSpoutPending(20); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		
		List<String> urls = new ArrayList<String>();
		urls.add( "rtsp://streaming3.webcam.nl:1935/n224/n224.stream" );
		urls.add("rtsp://streaming3.webcam.nl:1935/n233/n233.stream");
		urls.add("rtsp://streaming3.webcam.nl:1935/n302/n302.stream"); 
		urls.add("rtsp://streaming3.webcam.nl:1935/n346/n346.stream");
		urls.add("rtsp://streaming3.webcam.nl:1935/n319/n319.stream"); 
		urls.add("rtsp://streaming3.webcam.nl:1935/n794b/n794b.stream"); 

		int frameSkip = 13;
		
		// specify the list with SingleInputOperations to be executed sequentially by the 'fat' bolt
		@SuppressWarnings("rawtypes")
		List<ISingleInputOperation> operations = new ArrayList<ISingleInputOperation>();
		operations.add(new ScaleImageOp(0.5f) );
		operations.add(new FeatureExtractionOp("sift", FeatureDetector.SIFT, DescriptorExtractor.SIFT));
		operations.add(new FeatureExtractionOp("surf", FeatureDetector.SURF, DescriptorExtractor.SURF));
		operations.add(new DrawFeaturesOp());
		
		// now create the topology itself (spout -> background subtraction --> streamer)
		TopologyBuilder builder = new TopologyBuilder();
				
		// number of tasks must match the number of urls!
		builder.setSpout("spout", new CVParticleSpout( new StreamFrameFetcher(urls).frameSkip(frameSkip) ), 1 ).setNumTasks(6);
		
		// three 'fat' bolts containing a SequentialFrameOperation will will emit a Frame object containing the detected features
		builder.setBolt("features", new SingleInputBolt( new SequentialFrameOp(operations).outputFrame(true).retainImage(true)), 2)
			.shuffleGrouping("spout");
		
		// add bolt that creates a webservice on port 8558 enabling users to view the result
		builder.setBolt("streamer", new BatchInputBolt(
				new SlidingWindowBatcher(2, frameSkip).maxSize(6), // note the required batcher used as a buffer and maintains the order of the frames
				new MjpegStreamingOp().port(8558).framerate(5)).groupBy(new Fields(FrameSerializer.STREAMID))
			, 1)
			.shuffleGrouping("features");
		
		try {
			
			// run in local mode
			/*
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "deployment_Test", conf, builder.createTopology() );
			Utils.sleep(120*1000); // run for two minutes and then kill the topology
			cluster.shutdown();
			System.exit(1);
			*/
			// run on a storm cluster
			StormSubmitter.submitTopology("Your_topology_name", conf, builder.createTopology());
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
}

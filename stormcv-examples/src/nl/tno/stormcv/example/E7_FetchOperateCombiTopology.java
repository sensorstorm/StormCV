package nl.tno.stormcv.example;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.fetcher.FetchAndOperateFetcher;
import nl.tno.stormcv.fetcher.StreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.FeatureExtractionOp;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.SequentialFrameOp;
import nl.tno.stormcv.spout.CVParticleSpout;

import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class E7_FetchOperateCombiTopology {

	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		
		/**
		 * Sets the OpenCV library to be used which depends on the system the topology is being executed on
		 */
		//conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java2411.dylib");
		
		conf.setNumWorkers(3); // number of workers in the topology
		conf.setMaxSpoutPending(32); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib"); // sets the opencv lib to be used by all OpenCVOperation implementing operations
		
		List<String> urls = new ArrayList<String>();
		urls.add( "rtsp://streaming3.webcam.nl:1935/n224/n224.stream" );
		urls.add("rtsp://streaming3.webcam.nl:1935/n233/n233.stream");

		@SuppressWarnings("rawtypes")
		List<ISingleInputOperation> operations = new ArrayList<ISingleInputOperation>();
		operations.add(new FeatureExtractionOp("sift", FeatureDetector.SIFT, DescriptorExtractor.SIFT).outputFrame(true));
		operations.add(new DrawFeaturesOp() );
		 
		int frameSkip = 13; 
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new CVParticleSpout( 
				new FetchAndOperateFetcher( // use a meta fetcher to combine a Fetcher and Operation
						new StreamFrameFetcher(urls).frameSkip(frameSkip), // use a normal fetcher to get video frames from streams
						new SequentialFrameOp(operations).outputFrame(true).retainImage(true) // use a sequential operation to execute a number of operations on frames
					)
				) , 2 );
		
		// add bolt that creates a webservice on port 8558 enabling users to view the result
		builder.setBolt("streamer", new BatchInputBolt(
				new SlidingWindowBatcher(2, frameSkip).maxSize(6), // note the required batcher used as a buffer and maintains the order of the frames
				new MjpegStreamingOp().port(8558).framerate(5)).groupBy(new Fields(FrameSerializer.STREAMID))
			, 1)
			.shuffleGrouping("spout");
		
		try {
			
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "sequential_spout", conf, builder.createTopology() );
			Utils.sleep(120*1000); // run two minutes and then kill the topology
			cluster.shutdown();
			System.exit(1);
			
			// run on a storm cluster
			// StormSubmitter.submitTopology("some_topology_name", conf, builder.createTopology());
		} catch (Exception e){
			e.printStackTrace();
		}
	}

}

package nl.tno.stormcv.example;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.StreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.spout.CVParticleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class E1_GrayScaledTopology {

	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		conf.setNumWorkers(4); // number of workers in the topology
		conf.setMaxSpoutPending(32); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		
		List<String> urls = new ArrayList<String>();
		urls.add( "rtsp://streaming3.webcam.nl:1935/n224/n224.stream" );
		urls.add("rtsp://streaming3.webcam.nl:1935/n233/n233.stream");

		int frameSkip = 13;
		
		// now create the topology itself (spout -> scale --> grayscale --> streamer)
		TopologyBuilder builder = new TopologyBuilder();
		
		// just one spout reading streams; i.e. this spout reads two streams in parallel
		builder.setSpout("spout", new CVParticleSpout( new StreamFrameFetcher(urls).frameSkip(frameSkip) ), 1 );
		
		// add bolt that scales frames down to 66% of the original size 
		builder.setBolt("scale", new SingleInputBolt( new ScaleImageOp(0.66f)), 1)
			.shuffleGrouping("spout");
		
		// add bolt that converts color frames to spout 
		builder.setBolt("grayscale", new SingleInputBolt( new GrayscaleOp()), 1)
			.shuffleGrouping("scale");
		
		// add bolt that creates a web service on port 8558 enabling users to view the result
		builder.setBolt("streamer", new BatchInputBolt(
				new SlidingWindowBatcher(2, frameSkip).maxSize(6), // note the required batcher used as a buffer and maintains the order of the frames
				new MjpegStreamingOp().port(8558).framerate(5)).groupBy(new Fields(FrameSerializer.STREAMID))
			, 1)
			.shuffleGrouping("grayscale");
		
		// NOTE: if the topology is started (locally) go to http://localhost:8558/streaming/tiles and click the image to see the stream!
		
		try {
			
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "grayscaled", conf, builder.createTopology() );
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

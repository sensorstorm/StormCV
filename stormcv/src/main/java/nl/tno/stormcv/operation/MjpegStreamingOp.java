package nl.tno.stormcv.operation;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;
import com.sun.net.httpserver.HttpServer;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * A BatchInputBolt primarily used for testing that creates its own simple webservice used to view results MJPEG streams. The webservice 
 * supports a number of calls different calls:
 * <ol>
 * <li>http://IP:PORT/streaming/streams : lists the available JPG and MJPEG urls</li>
 * <li>http://IP:PORT/streaming/picture/{streamid}.jpg : url to grab jpg pictures </li>
 * <li>http://IP:PORT/streaming/tiles : provides a visual overview of all the streams available at this service. Clicking an image will open the mjpeg stream</li>
 * <li>http://IP:PORT/streaming/mjpeg/{streamid}.mjpeg : provides a possibly never ending mjpeg formatted stream</li>
 * </ol> 
 * 
 * The service runs on port 8558 by default but this can be changed by using the port(int) method.
 * 
 * @author Corne Versloot
 *
 */

@SuppressWarnings("restriction")
@Path("/streaming")
public class MjpegStreamingOp extends Application implements IBatchOperation<Frame> {

	private static final long serialVersionUID = -4558017042873627826L;
	private static Cache<String, BufferedImage> images = null;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private HttpServer server;
	private int port = 8558;
	private int frameRate = 2;
	
	private static Cache<String, BufferedImage> getImages(){
		if(images == null){
			images = CacheBuilder.newBuilder()
					.expireAfterWrite(20, TimeUnit.SECONDS) 
					.build();
		}
		return images;
	}

	public MjpegStreamingOp port(int nr){
		this.port = nr;
		return this;
	}
	
	public MjpegStreamingOp framerate(int nr){
		this.frameRate = nr;
		return this;
	}
	
	/**
	 * Sets the classes to be used as resources for this application
	 */
	public Set<Class<?>> getClasses() {
        Set<Class<?>> s = new HashSet<Class<?>>();
        s.add(MjpegStreamingOp.class);
        return s;
    }
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context)	throws Exception {
		images = MjpegStreamingOp.getImages();
		
		ApplicationAdapter connector = new ApplicationAdapter(new MjpegStreamingOp());
		server = HttpServerFactory.create ("http://localhost:"+port+"/", connector);
		server.start();		
	}

	@Override
	public void deactivate() {
		server.stop(0);
		images.invalidateAll();
		images.cleanUp();
	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@Override
	public List<Frame> execute(List<CVParticle> input) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		for(int i=0; i<input.size(); i++){
			CVParticle s = input.get(i);
			if(!(s instanceof Frame)) continue;
			Frame frame = (Frame)s;
			result.add(frame);
			if(frame.getImage() == null) continue;
			//BufferedImage prevImage = images.getIfPresent(frame.getStreamId());
			images.put(frame.getStreamId(), frame.getImage());
			/*
			if(prevImage != null) synchronized (prevImage){ prevImage.notifyAll(); }// notify all listeners that they can grab a new image
			//System.err.println("Add frame ["+frame.getSequenceNr()+"] after "+(System.currentTimeMillis() - prevAdd)+" ms");
			prevAdd = System.currentTimeMillis();
			*/
			break;
		}
		return result;
	}
	
	@GET @Path("/streams")
	@Produces("text/plain")
	public String getStreamIds() throws IOException{
		String result = new String();
		for(String id : images.asMap().keySet()){
			result += "/streaming/picture/"+id+".jpeg\r\n";
		}
		System.out.println("\r\n");
		for(String id : images.asMap().keySet()){
			result += "/streaming/mjpeg/"+id+".mjpeg\r\n";
		}
		return result;
	}
	
	@GET @Path("/picture/{streamid}.jpeg")
	@Produces("image/jpg")
	public Response jpeg(@PathParam("streamid") final String streamId){
		BufferedImage image = null;
		if((image = images.getIfPresent(streamId)) != null){
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				ImageIO.write(image, "jpg", baos);
				byte[] imageData = baos.toByteArray();
				return Response.ok(imageData).build(); // non streaming
				// return Response.ok(new ByteArrayInputStream(imageDAta)).build(); // streaming
			} catch (IOException ioe) {
				logger.warn("Unable to write image to output", ioe);
				return Response.serverError().build();
			}
		}else{
			return Response.noContent().build();
		}
	}
	
	@GET @Path("/playmultiple")
	@Produces("text/html")
	public String showPlayers( @DefaultValue("3") @QueryParam("cols") int cols,
			 @DefaultValue("0") @QueryParam("offset") int offset,
			 @DefaultValue("6") @QueryParam("number") int number) throws IOException{
		//number = Math.min(6, number);
		String result = "<html><head><title>Mjpeg stream players</title></head><body bgcolor=\"#3C3C3C\">";
		result += "<font style=\"color:#CCC;\">Streams: "+images.size()+" (showing "+offset+" - "+Math.min(images.size(), offset+number)+")</font><br/>";
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		int videoNr = 0;
		for(String id : images.asMap().keySet()){
			if(videoNr < offset ){
				videoNr++;
				continue;
			}
			if(videoNr-offset > 0 &&(videoNr-offset) % cols == 0){
				result+="</tr><tr>";
			}
			result += "<td><video poster=\"mjpeg/"+id+".mjpeg\">"+
					"Your browser does not support the video tag.</video></td>";
			//result += "<td><img src=\"http://"+InetAddress.getLocalHost().getHostAddress()+":"+port+"/streaming/mjpeg/"+id+".mjpeg\"></td>";
			if(videoNr > offset + number) break;
			videoNr++;
		}
		result += "</tr></table></body></html>";
		return result;
	}
	
	@GET @Path("/play")
	@Produces("text/html")
	public String showPlayers( @QueryParam("streamid") String streamId) throws IOException{
		String result = "<html><head><title>Mjpeg stream: "+streamId+"</title></head><body bgcolor=\"#3C3C3C\">";
		result += "<font style=\"color:#CCC;\"><a href=\"tiles\">Back</a></font><br/>";
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		result += "<video poster=\"mjpeg/"+streamId+".mjpeg\">"+
					"Your browser does not support the video tag.</video>";
		return result;
	}
	
	@GET @Path("/tiles")
	@Produces("text/html")
	public String showTiles( @DefaultValue("3") @QueryParam("cols") int cols,
			@DefaultValue("-1") @QueryParam("width") float width) throws IOException{
		String result = "<html><head><title>Mjpeg stream players</title>";
		result += "</head><body bgcolor=\"#3C3C3C\">";
		
		result += "<table style=\"border-spacing:0; border-collapse: collapse;\"><tr>";
		int videoNr = 0;
		for(String id : images.asMap().keySet()){
			if(videoNr > 0 && videoNr % cols == 0){
				result+="</tr><tr>";
			}
			result += "<td><a href=\"play?streamid="+id+"\"><img src=\"picture/"+id+".jpeg\" "+(width > 0 ? "width=\""+width+"\"" : "")+"/></a>";
			videoNr++;
		}
		result += "</tr></table></body></html>";
		return result;
	}
	
	@GET @Path("/mjpeg/{streamid}.mjpeg")
	@Produces("multipart/x-mixed-replace; boundary=--BoundaryString\r\n")
	public Response mjpeg(@PathParam("streamid") final String streamId){
		StreamingOutput output = new StreamingOutput() {
			
			private BufferedImage prevImage = null;
			private int sleep = 1000/frameRate;
			
			@Override
			public void write(OutputStream outputStream) throws IOException, WebApplicationException {
				BufferedImage image = null;
				try{
					while((image = images.getIfPresent(streamId)) != null) /*synchronized(image)*/ {
						if(prevImage == null || !image.equals(prevImage)){
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
							ImageIO.write(image, "jpg", baos);
							byte[] imageData = baos.toByteArray();
							 outputStream.write((
								        "--BoundaryString\r\n" +
								        "Content-type: image/jpeg\r\n" +
								        "Content-Length: "+imageData.length+"\r\n\r\n").getBytes());
							outputStream.write(imageData);
							outputStream.write("\r\n\r\n".getBytes());
							outputStream.flush();
						}
						Utils.sleep(sleep);
						/*
						try {
							image.notifyAll();
							image.wait();
						} catch (InterruptedException e) {
							// just read the next image
						}
						*/
					}
					outputStream.flush();
					outputStream.close();
				}catch(IOException ioe){
					logger.info("Steam for ["+streamId+"] closed by client!");
				}
			}
		};
		return Response.ok(output)
				.header("Connection", "close")
				.header("Max-Age", "0")
				.header("Expires", "0")
				.header("Cache-Control", "no-cache, private")
				.header("Pragma", "no-cache")
				.build();
	}

}

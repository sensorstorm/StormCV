package nl.tno.stormcv.util;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import nl.tno.stormcv.model.Frame;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

/**
 * A utility class to convert images to bytes and vice-versa which is primarily used
 * for serialization from/to Tuple's 
 * 
 * @author Corne Versloot
 */
public class ImageUtils {

	/**
	 * Converts an image to byte buffer representing PNG (bytes as they would exist on disk)
	 * @param image
	 * @param encoding the encoding to be used, one of: png, jpeg, bmp, wbmp, gif
	 * @return byte[] representing the image
	 * @throws IOException if the bytes[] could not be written
	 */
	public static byte[] imageToBytes(BufferedImage image, String encoding) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(image, encoding, baos);
		return baos.toByteArray();
	}
	
	/**
	 * Converts the provided byte buffer into an BufferedImage
	 * @param buf byte[] of an image as it would exist on disk
	 * @return
	 * @throws IOException
	 */
	public static BufferedImage bytesToImage(byte[] buf) throws IOException{
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		return ImageIO.read(bais);
	}
	
	/**
	 * Converts a given image into grayscalse
	 * @param src
	 * @return
	 */
	public static BufferedImage convertToGray(BufferedImage src){
        ColorConvertOp grayOp = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
        return grayOp.filter(src,null);
    }
	
	/**
	 * Creates a Mat object for the image in the provided frame
	 * @param frame
	 * @return Mat object representing the image of type Highgui.CV_LOAD_IMAGE_COLOR
	 * @throws IOException if the image cannot be read or converted into binary format
	 */
	public static Mat Frame2Mat(Frame frame) throws IOException{
		if(frame.getImage() == null) throw new IOException("Frame does not contain an image");
		return bytes2Mat(frame.getImageBytes());
	}
	
	/**
	 * Creates a Mat object for the image in the provided frame
	 * @param image the image to be converted to mat
	 * @param imageType the encoding to use, see {@link Frame}
	 * @return Mat object representing the image of type Highgui.CV_LOAD_IMAGE_COLOR
	 * @throws IOException if the image cannot be read or converted into binary format
	 */
	public static Mat Image2Mat(BufferedImage image, String imageType) throws IOException{
		MatOfByte mob = new MatOfByte( ImageUtils.imageToBytes(image, imageType) );
		return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
	}
	
	/**
	 * creates a Mat object directly from a set of bytes 
	 * @param bytes binary representation of an image
	 * @return Mat object of type Highgui.CV_LOAD_IMAGE_COLOR
	 */
	public static Mat bytes2Mat(byte[] bytes){
		MatOfByte mob = new MatOfByte( bytes );
		return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
	}
	
	/**
	 * Creates a byte representation of the provided mat object encoded using the imageType
	 * @param mat
	 * @param imageType
	 * @return
	 */
	public static byte[] Mat2ImageBytes(Mat mat, String imageType){
		MatOfByte buffer = new MatOfByte();
		Highgui.imencode("."+imageType, mat, buffer);
		return buffer.toArray();
	}
}

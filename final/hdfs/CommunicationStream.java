package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import config.GeneralConfig;

public class CommunicationStream {

	private Socket penpal;
	private InputStream input;
	private OutputStream output;
	
	public CommunicationStream(Socket penpal) {
		try {
			this.penpal = penpal;
			this.input = this.penpal.getInputStream();
			this.output = this.penpal.getOutputStream();
		} catch(IOException e) {e.printStackTrace();}
	}
	
	public byte[] receiveData() throws IOException {
		int length = this.receiveDataInt();
		if (length > 0) {
			byte[] data = new byte[length];
			this.input.read(data, 0, length);
			return data;
		} else {
			return null;
		}
	}
	
	public int receiveDataInt() throws IOException {
		byte[] data = new byte[GeneralConfig.BytesInt];
		int numberRead = this.input.read(data, 0, GeneralConfig.BytesInt);
		if (numberRead > 0) {
			ByteBuffer converter = ByteBuffer.wrap(data);
			return converter.getInt();
		} else {
			return -1;
		}
	}
	
	public void sendData(byte[] data, int length) throws IOException {
		this.sendData(length);
		this.output.write(data, 0, length);
	}
	
	public void sendData(int data) throws IOException {
		ByteBuffer converter = ByteBuffer.allocate(GeneralConfig.BytesInt);
		converter.putInt(data);
		this.output.write(converter.array());
	}
	
	public void close() throws IOException {
		this.penpal.close();
		this.input.close();
		this.output.close();
	}
	
}

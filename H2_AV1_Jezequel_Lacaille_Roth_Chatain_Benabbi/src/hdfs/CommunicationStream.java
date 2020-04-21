package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;

import config.ClusterConfig;
import config.GeneralConfig;
import hdfs.daemon.FragmentDataI;
import hdfs.server.FileDataI;

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
	
	public FileDescriptionI receiveFile() throws IOException {
        String name, path, alias, destination;
        alias = destination = null;
        name = this.receiveDataString();
        path = this.receiveDataString();
        if (this.receiveDataByte() == 1)
        	alias = this.receiveDataString();
        if (this.receiveDataByte() == 1)
        	path = this.receiveDataString();
        return GeneralConfig.getFileDescription(name, path, alias, destination);
	}
	
	public FileDataI receiveFileData() throws IOException {
		FileDescriptionI file = this.receiveFile();
		FileDataI data = ClusterConfig.getFileData(file);
        int nodeNumber = this.receiveDataInt();
		for (int i = 0; i < nodeNumber; i++) {
			int node = this.receiveDataInt();
			int fragmentNumber = this.receiveDataInt();
			for (int j = 0; j < fragmentNumber; j++)
				data.addDaemonFragment(node, this.receiveDataInt());
		}
		return data;
	}
	
	public FragmentDataI receiveFragmentData() throws IOException {
		FileDescriptionI file = this.receiveFile();
		String path = this.receiveDataString();
		FragmentDataI data = ClusterConfig.getFragmentData(file, path);
		int fragmentNumber = this.receiveDataInt();
		for (int i = 0; i < fragmentNumber; i++) {
			int fragment = this.receiveDataInt();
			String fragmentName = new String(this.receiveData());
			data.addFragment(fragment, fragmentName);
		}
        return data;
	}
	
	public Command receiveCommand() throws IOException {
		return Command.intToCommand(this.receiveDataInt());
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
	
	public byte receiveDataByte() throws IOException {
		byte[] data = new byte[1];
		int numberRead = this.input.read(data, 0, 1);
		if (numberRead > 0) {
			ByteBuffer converter = ByteBuffer.wrap(data);
			return converter.get();
		} else {
			return -1;
		}
	}
	
	public String receiveDataString() throws IOException {
		byte[] data = this.receiveData();
		if (data != null)
			return new String(data);
		else 
			return null;
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
	
	public long receiveDataLong() throws IOException {
		byte[] data = new byte[GeneralConfig.BytesLong];
		int numberRead = this.input.read(data, 0, GeneralConfig.BytesLong);
		if (numberRead > 0) {
			ByteBuffer converter = ByteBuffer.wrap(data);
			return converter.getLong();
		} else {
			return -1;
		}
	}
	
	public float receiveDataFloat() throws IOException {
		byte[] data = new byte[GeneralConfig.BytesFloat];
		int numberRead = this.input.read(data, 0, GeneralConfig.BytesFloat);
		if (numberRead > 0) {
			ByteBuffer converter = ByteBuffer.wrap(data);
			return converter.getLong();
		} else {
			return -1;
		}
	}
	
	public double receiveDataDouble() throws IOException {
		byte[] data = new byte[GeneralConfig.BytesDouble];
		int numberRead = this.input.read(data, 0, GeneralConfig.BytesDouble);
		if (numberRead > 0) {
			ByteBuffer converter = ByteBuffer.wrap(data);
			return converter.getLong();
		} else {
			return -1;
		}
	}
	
	public void sendData(FileDescriptionI file) throws IOException {
		this.sendData(file.getName());
		this.sendData(file.getPath());
        if (file.hasAlias()) {
        	this.sendData((byte)1);
			this.sendData(file.getAlias());
        } else
        	this.sendData((byte)0);
        if (file.hasDestinationName()) {
        	this.sendData((byte)1);
			this.sendData(file.getDestinationName());
        } else
        	this.sendData((byte)0);
	}
	
	public void sendData(FileDataI data) throws IOException {
		this.sendData(data.getFile());
		this.sendData(data.getNumberDaemons());
		for (Integer node : data) {
			this.sendData(node);
			Collection<Integer> fragments = data.getDaemonFragments(node);
			this.sendData(fragments.size());
			for (Integer fragment : fragments) 
				this.sendData(fragment);
		}
	}
	
	public void sendData(FragmentDataI data) throws IOException {
		this.sendData(data.getFile());
		this.sendData(data.getFragmentsPath());
		this.sendData(data.getNumberFragments());
		for (Integer fragment : data) {
			this.sendData(fragment);
			this.sendData(data.getFragmentName(fragment));
		}
	}
	
	public void sendData(Command command) throws IOException {
		this.sendData(command.toInt());
	}
	
	public void sendData(byte[] data, int length) throws IOException {
		this.sendData(length);
		this.output.write(data, 0, length);
	}
	
	public void sendData(byte data) throws IOException {
		this.output.write(data);
	}
	
	public void sendData(String data) throws IOException {
		byte[] buffer = data.getBytes();
		this.sendData(buffer, buffer.length);
	}
	
	public void sendData(int data) throws IOException {
		ByteBuffer converter = ByteBuffer.allocate(GeneralConfig.BytesInt);
		converter.putInt(data);
		this.output.write(converter.array());
	}
	
	public void sendData(long data) throws IOException {
		ByteBuffer converter = ByteBuffer.allocate(GeneralConfig.BytesLong);
		converter.putLong(data);
		this.output.write(converter.array());
	}
	
	public void sendData(float data) throws IOException {
		ByteBuffer converter = ByteBuffer.allocate(GeneralConfig.BytesFloat);
		converter.putFloat(data);
		this.output.write(converter.array());
	}
	
	public void sendData(double data) throws IOException {
		ByteBuffer converter = ByteBuffer.allocate(GeneralConfig.BytesDouble);
		converter.putDouble(data);
		this.output.write(converter.array());
	}
	
	public void close() throws IOException {
		this.penpal.close();
		this.input.close();
		this.output.close();
	}
	
}

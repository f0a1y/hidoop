package hdfs.fileProcessing;

import java.util.List;

public class FileBreakerTXT extends FileBreaker {
	
	public FileBreakerTXT(int fragmentLength) {
		super(fragmentLength);
	}
	
	@Override
	public int fragment(byte[] data, int length, List<byte[]> fragments) {
		int index = 0;		
		String remainder = null;
		StringBuilder fragment = new StringBuilder();
		while (index + this.getFragmentLength() <= length) {			
			fragment.append(new String(data, index, this.getFragmentLength()));
			int section = fragment.lastIndexOf(" ");
			fragments.add(fragment.substring(0, section).getBytes());
			index += this.getFragmentLength();
			remainder = fragment.substring(section);
			fragment.setLength(0);
			fragment.append(remainder);
		}
		if (index > 0) {
			return index - remainder.getBytes().length;
		}
		return 0;
	}

}

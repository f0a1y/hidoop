package hdfs.fileProcessing;

import java.util.List;

public class FileBreakerTXT extends FileBreakerA {
	
	public FileBreakerTXT(int fragmentLength) {
		super(fragmentLength);
	}
	
	@Override
	public boolean fragment(byte[] data, int limit, int firstFragmentLength, List<byte[]> fragments) {
		int index, current;
		index = current = 0;
		int length = Math.max(this.getFragmentLength() - firstFragmentLength, 10);
		StringBuilder content = new StringBuilder();
		while (current + length <= limit) {
			content.append(new String(data, current, length));
			int separation = content.lastIndexOf(" ") + 1;
			if (separation > 0) {
				fragments.add(content.substring(0, separation).getBytes());
				content = new StringBuilder();
				index += separation;
				current = index;
				length = this.getFragmentLength();
			}
			else {
				current += length;
				length = 10;
			}
		}
		if (index == limit)
			return true;
		else {
			content.append(new String(data, current, limit - current));
			fragments.add(content.toString().getBytes());
			return false;
		}
	}
	
}

package formats;

public class FormatSelectorBasic implements FormatSelectorI {
	
	@Override
	public Format selectFormat(String fileName) {
		if (fileName.regionMatches(true, fileName.lastIndexOf('r'), "res", 0, 3)
			|| fileName.regionMatches(true, fileName.lastIndexOf('r'), "resTemp", 0, 7))
			return new KVFormat(fileName);
		else if (fileName.regionMatches(true, fileName.lastIndexOf('.'), ".txt", 0, 4))
			return new LineFormat(fileName);
		else if (fileName.regionMatches(true, fileName.lastIndexOf('.'), "kv", 0, 3))
			return new KVFormat(fileName);
		else
			return new KVFormat(fileName);
	}

}

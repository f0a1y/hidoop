package formats;

public class FormatSelectorBasic implements FormatSelectorI {
	
	@Override
	public Format selectFormat(String fileName) {
		if (fileName.matches("(.*)\\.(.*)res(.*)") 
		    || fileName.matches("(.*)\\.(.*)resTemp(.*)")
		    || fileName.matches("(.*)\\.kv(.*)"))
			return new KVFormat(fileName);
		else if (fileName.matches("(.*)\\.txt(.*)"))
			return new LineFormat(fileName);
		else
			return null;
	}

}

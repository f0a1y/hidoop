package formats;

import formats.Format.Type;

public class FormatSelector implements FormatSelectorI {
	
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
	
	public Format selectFormat(Type type, String fileName) {
		switch(type)	{
		case KV : 
			return new KVFormat(fileName);
		case LINE : 
			return new LineFormat(fileName);	
		default :
			return new KVFormat(fileName);
		}
	}

}

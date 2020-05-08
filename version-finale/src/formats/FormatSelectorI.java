package formats;

import formats.Format.Type;

public interface FormatSelectorI {
	
	Format selectFormat(String fileName);
	
	Format selectFormat(Type type, String fileName);
	
}

package hdfs;

public class FileDescription implements FileDescriptionI {

	private static final long serialVersionUID = 1L;
	private String name;
	private String path;
	private String alias;
	private String destinationName;
	
	public FileDescription(String name, String path, String alias, String destinationName) {
		this.name = name;
		this.path = path;
		this.alias = alias;
		this.destinationName = destinationName;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getPath() {
		return this.path;
	}
	
	public boolean hasAlias() {
		return this.alias != null;
	}
	
	public String getAlias() {
		return this.alias;
	}
	
	public boolean hasDestinationName() {
		return this.destinationName != null;
	}
	
	public String getDestinationName() {
		return this.destinationName;
	}
	
	public void update(FileDescriptionI file) {
		this.name = file.getName();
		this.alias = file.getAlias();
	}
	
	@Override
	public boolean equals(Object other) {
		boolean match = false;
		if (other != null) {
			if (this == other) 
				match = true;
			else {
				if (other instanceof FileDescriptionI) {
					FileDescriptionI file = (FileDescriptionI)other;
					if (file.hasAlias()) {
						if (this.hasAlias())
							match = this.alias.equals(file.getAlias());
						else 
							match = this.name.equals(file.getAlias());
					} else {
						if (this.hasAlias())
							match = this.alias.equals(file.getName());
						else
							match = this.name.equals(file.getName()) && this.path.equals(file.getPath());
					}
				}
			}
		}
		return match;
	}
	
	@Override
	public int hashCode() {
		int hash = this.hasAlias() ? this.alias.hashCode() :  this.name.hashCode();
		return hash;
	}
	
	public String toString() {
		StringBuilder description = new StringBuilder();
		description.append('[');
		description.append(this.path);
		description.append(" ");
		description.append(this.name);
		if (this.alias != null) 
			description.append(" alias " + this.alias);
		if (this.destinationName != null)
			description.append(" -> " + this.destinationName);
		description.append(']');
		return description.toString();
	}
	
}

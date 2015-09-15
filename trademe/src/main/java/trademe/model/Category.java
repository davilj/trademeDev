package trademe.model;

public class Category {
	//match json tags name
	public static final String NAME="Name";
	public static final String ID = "Number";
	public static final String PATH = "Path";
	
	private String name;
	private String path;
	private String id;
	
	public String getName() {
		return name;
	}
	
	public String getPath() {
		return path;
	}
	
	public String getId() {
		return id;
	}
	
	public void setProp(String name, String value) {
		switch (name) {
		case NAME:
			this.name = value;
			break;
		case PATH:
			this.path = value;
			break;
		case ID:
			this.id = value;
		}
	}
	
	@Override
	public String toString() {
		return "Category [name=" + name + ", path=" + path + ", id=" + id + "]";
	}

	public boolean isMainCategory() {
		if (id.equals("")) return false;
		return this.id.split("-").length==1;
	}
	
	
}

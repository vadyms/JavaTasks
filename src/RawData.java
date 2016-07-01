import java.util.HashMap;

public class RawData {
	
	private HashMap<String, String> members;
	
	public RawData() {
		members=new HashMap<>();
	}

	public void addMember(String name, String value) {
		members.put(name, value);
	}
	
	public String getMember(String name) {
		return members.get(name);
	}
}
package followers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TwitterUserWritable implements Writable {

	private String user;
	private Integer noOfFollowers;
	
	public TwitterUserWritable() {
		
	}
	
	public TwitterUserWritable(String user, Integer noOfFollowers) {
		super();
		this.user = user;
		this.noOfFollowers = noOfFollowers;
	}	
	

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Integer getNoOfFollowers() {
		return noOfFollowers;
	}

	public void setNoOfFollowers(Integer noOfFollowers) {
		this.noOfFollowers = noOfFollowers;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.user = arg0.readUTF();
		this.noOfFollowers = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(user);
		arg0.writeInt(noOfFollowers);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((noOfFollowers == null) ? 0 : noOfFollowers.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwitterUserWritable other = (TwitterUserWritable) obj;
		if (noOfFollowers == null) {
			if (other.noOfFollowers != null)
				return false;
		} else if (!noOfFollowers.equals(other.noOfFollowers))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return  noOfFollowers + "\t" + user ;
	}
	
	
	
	

}

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {
	public Text tag;
	public int count;
	
	public StringAndInt() {
		this.tag = new Text("");
		this.count = 0;
		
	}
	
	public StringAndInt(Text s) {
		this.tag = s;
		this.count = 0;
	}
	
	public StringAndInt(Text s, int i) {
		this.tag = s;
		this.count = i;
	}
	
	// Ordonner le pays
	@Override
	public int compareTo(StringAndInt o) {
		return o.count - this.count;
	}
	
	@Override
	public String toString() {
		//  U200E : CASTING LEFT TO RIGHT FOR ARABIC CHARACTERS => Written right to left and words ordered left to right
		return this.tag.toString()+"\u200E:"+this.count;
	}


	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.tag.readFields(arg0);
		this.count = arg0.readInt();
	}


	@Override
	public void write(DataOutput arg0) throws IOException {
		this.tag.write(arg0);
		arg0.writeInt(this.count);
	}

}

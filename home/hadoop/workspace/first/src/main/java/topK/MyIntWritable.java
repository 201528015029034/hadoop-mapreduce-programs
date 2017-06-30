package topK;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * define MyIntWritable, which sort from big to small
 * @author hadoop
 *
 */
public class MyIntWritable implements WritableComparable<MyIntWritable> {
	private Integer num;
	
	public MyIntWritable() {
	}
	
	public MyIntWritable(Integer num) {
		this.num = num;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(num);
	}

	public void readFields(DataInput in) throws IOException {
		this.num = in.readInt();
	}

	public int compareTo(MyIntWritable o) {
		return -1 * (this.num - o.num);
	}
	
	@Override
	public int hashCode() {
		return this.num.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof MyIntWritable)) {
			return false;
		}
		MyIntWritable mi = (MyIntWritable)obj;
		return (this.num == mi.num);
	}
	
	@Override
	public String toString() {
		return num + "";
	}
}

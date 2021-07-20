import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FloatPair implements WritableComparable<FloatPair> {
	private float first;
	private float second;
	
	public void set(float left, float right) {
		first = left;
		second = right;
	}
	
	public float getFirst() {
		return first;
	}
	public float getSecond() {
		return second;
	}
	
	public void readFields(DataInput in) throws IOException {
		first = in.readFloat();
		second = in.readFloat();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(first);
		out.writeFloat(second);
	}

	public int compareTo(FloatPair arg) {
		if (first != arg.first) {
			return (first - arg.first > 0) ? 1 : -1;
		}
		else if (second != arg.second) {
			return (arg.second - second > 0) ? 1 : -1;
		}
		else return 0;
	}
}

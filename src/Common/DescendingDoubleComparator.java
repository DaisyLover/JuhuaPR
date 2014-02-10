package Common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ziyu on 2/10/14.
 */
public class DescendingDoubleComparator implements RawComparator<DoubleWritable>{
    private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();

    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return -DOUBLE_COMPARATOR.compare(bytes, i, i2, bytes2, i3, i4);
    }

    @Override
    public int compare(DoubleWritable doubleWritable, DoubleWritable doubleWritable2) {
        return -DOUBLE_COMPARATOR.compare(doubleWritable, doubleWritable2);
    }

    @Override
    public boolean equals(Object obj) {
        return DOUBLE_COMPARATOR.equals(obj);
    }
}


package Common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ziyu on 2/10/14.
 */
public class ComparableWritable{
    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(Text.class, true);
        }


        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object a, Object b) {
            return -super.compare(a, b);
        }
    }
}


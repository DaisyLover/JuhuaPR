package test;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ziyu on 2/7/14.
 */
public class XmlWikiPageTest {
    @Test
    public void testPatternMatching(){
        Pattern pattern = Pattern.compile("\\[([^:#/\\|\\[\\]]+)\\|?(?:[^:#/\\[\\]]*)\\]");
        String testString = "[abc|d]";
        Matcher matcher = pattern.matcher(testString);
        matcher.find();
        matcher.groupCount();
    }

    @Test
    public void testTextEqual(){
        String a = "a";
        String b = "a";
        Text A = new Text(a);
        Text B = new Text(b);
        Assert.assertTrue("Error: A not equals to B!", A.equals(B));
        Assert.assertTrue("Error: A.hashcode != B.hashcode!", A.hashCode()==B.hashCode());
    }

    @Test
    public void testStringFormat(){
        String s1 = String.format("results/PageRank.iter%d.out", 1);
        Assert.assertTrue("Error", s1.equals("results/PageRank.iter1.out"));
    }

    @Test
    public void testZYSB(){
        StringBuilder sb = new StringBuilder();
        Assert.assertTrue("True", "".equals(sb.toString()));
    }

}

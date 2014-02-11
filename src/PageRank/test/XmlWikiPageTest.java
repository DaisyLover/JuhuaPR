package PageRank.test;

import PageRank.Common.PageRankedWikiPage;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.CharacterCodingException;
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
    public void testWikiPageRestore(){
        String input = "XSL_attack\t1.00\tCryptanalysis\tBlock_cipher\tAdvanced_Encryption_Standard";
        PageRankedWikiPage page = new PageRankedWikiPage();
        page.restoreFromString(input);
        Assert.assertTrue(page.getTitle().equals("XSL_attack"));
        Assert.assertTrue(page.getPageRank() == 1.00);
        Assert.assertTrue(page.getOutLinks().size() == 3);
    }

    @Test
    public void testStringContains(){
        String input = "XSL_attack\t1.00\tCryptanalysis\tBlock_cipher\tAdvanced_Encryption_Standard";
        Assert.assertTrue(input.contains("\t"));
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
        String s1 = String.format("results/PageRank.PageRank.iter%d.out, %.2f", 1, 3.1415926535);
        Assert.assertTrue("Error", s1.equals("results/PageRank.PageRank.iter1.out, 3.14"));
    }

    @Test
    public void testTextDecode() throws CharacterCodingException {
        Text value = new Text("title\tpage1\tpage2");
        int start = value.find("\t");
        String title = Text.decode(value.getBytes(), 0, start);
        String links = Text.decode(value.getBytes(), start+1, value.getLength() - start - 1);
        Assert.assertTrue(title.equals("title"));
        Assert.assertTrue(links.equals("page1\tpage2"));
    }

    @Test
    public void testZYSB(){
        StringBuilder sb = new StringBuilder();
        Assert.assertTrue("True", "".equals(sb.toString()));
    }

    @Test
    public void testZYSB2(){
        String sbzy = "erew\t";
        String[] structure = sbzy.split("\\t");
        Assert.assertTrue("True", structure.length == 1);
    }

    @Test
    public void testZYSB3(){
        StringBuilder inlinkGraph = new StringBuilder();
        Assert.assertTrue("True", "".equals(inlinkGraph.toString()));
    }

    @Test
    public void testZYSB4(){
        String sbzy = "er\t e w\t";
        Assert.assertTrue("True", "er__e_w_".equals(sbzy.replaceAll("[\\s\\t]", "_")));
    }

    @Test
    public void testBrada(){
        double pageRank = 1.0;
        pageRank /= 0.0;
        Assert.assertTrue("Error", pageRank == 1.0/3);
    }


}

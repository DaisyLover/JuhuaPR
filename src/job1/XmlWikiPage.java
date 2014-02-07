package job1;

import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;

/**
 * Created by Ziyu on 2/7/14.
 */
public class XmlWikiPage {
    public static final String TITLE_START_TAG = "<title>";
    public static final String TITLE_END_TAG = "</title>";

    private Text pageText = null;
    private Text title = null;
    private ArrayList<Text> links = new ArrayList<Text>();

    public Text getTitle() {
        return title;
    }

    public ArrayList<Text> getLinks() {
        return links;
    }

    public XmlWikiPage(Text pageText){
        this.pageText = pageText;
    }

    public void parse() throws CharacterCodingException {
        parseTitle();
        parseWikiLinks();
    }

    private void parseTitle() throws CharacterCodingException {
        int start = pageText.find("<title>");
        int end = pageText.find("</title>", start);
        start += 7; //add <title> length.
        title = new Text(Text.decode(pageText.getBytes(), start, end - start));
    }

    private void parseWikiLinks(){

    }

    private boolean isWikiLink(String link){
        return false;
    }

    private String extractLinkedPageTitle(String link){
        return null;
    }
}

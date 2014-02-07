package job1;

import org.apache.hadoop.io.Text;

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

    public void parse(){
        parseTitle();
        parseWikiLinks();
    }

    private void parseTitle(){

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

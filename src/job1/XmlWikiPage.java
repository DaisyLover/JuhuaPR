package job1;

import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Ziyu on 2/7/14.
 */
public class XmlWikiPage {
    public static final String TITLE_START_TAG = "<title>";
    public static final String TITLE_END_TAG = "</title>";
    public static final String WIKI_LINK_PATTERN = "\\[([^:#/]+)\\|?(?:[^:#/]+)\\]";

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
        int start = pageText.find(TITLE_START_TAG);
        int end = pageText.find(TITLE_END_TAG, start);
        start += 7; //<title> length.
        String titleString = Text.decode(pageText.getBytes(), start, end - start);
        titleString = titleString.replaceAll("\\s", "_");
        this.title = new Text(titleString);
    }

    private void parseWikiLinks() throws CharacterCodingException {
        int start = pageText.find("<text");
        start = pageText.find(">", start);  //<text ...> might contain attributes
        int end = pageText.find("</text>", start);
        start += 1; //skip ">"
        String contentString = Text.decode(pageText.getBytes(), start, end - start);
        Pattern wikiLinkPattern = Pattern.compile(WIKI_LINK_PATTERN);
        Matcher matcher = wikiLinkPattern.matcher(contentString);

        //find wiki links and add to arraylist
        while (matcher.find()) {
            String linkedPage = matcher.group();
            linkedPage = linkedPage.replaceAll("\\s", "_");
            if(linkedPage == null || linkedPage.isEmpty())
                continue;
            this.links.add(new Text(linkedPage));
        }
    }
}

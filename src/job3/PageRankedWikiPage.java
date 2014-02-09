package job3;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Ziyu on 2/8/14.
 */
public class PageRankedWikiPage {
    private double pageRank = 0;
    private String title = null;
    private ArrayList<String> outLinks = new ArrayList<String>();

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public ArrayList<String> getOutLinks() {
        return outLinks;
    }

    public void setOutLinks(ArrayList<String> outLinks) {
        this.outLinks = outLinks;
    }

    public void restoreFromString(String structure){
        String[] tmp = structure.split("\t");
        title = tmp[0];
        pageRank = Double.parseDouble(tmp[1]);
        outLinks.clear();
        outLinks.addAll(Arrays.asList(Arrays.copyOfRange(tmp, 2, tmp.length)));
    }

    public String serializeGrapnInfo(){
        StringBuilder builder = new StringBuilder();
        char seperator = '\t';
        builder.append(pageRank);
        for(String outLink: outLinks){
            builder.append(seperator).append(outLink);
        }
        return builder.toString();
    }
}

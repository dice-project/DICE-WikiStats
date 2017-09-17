package it.polimi.deib.dice.spark;

import java.io.Serializable;

/**
 * Created by miik on 16/09/17.
 */
public class Page implements Serializable {

    private String pageId;
    private String pageTitle;
    private String pageContent;

    public Page(String pageId, String pageTitle, String content){
        this.pageId = pageId;
        this.pageTitle = pageTitle;
        this.pageContent = content;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }

    public String getPageContent() {
        return pageContent;
    }

    public void setPageContent(String pageContent) {
        this.pageContent = pageContent;
    }

}

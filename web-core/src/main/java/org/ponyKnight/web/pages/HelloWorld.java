package org.ponyKnight.web.pages;

import org.apache.click.Page;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-2-23
 * Time: 下午6:30
 * To change this template use File | Settings | File Templates.
 */
public class HelloWorld extends Page {
    private Date time = new Date();

    public HelloWorld() {
        addModel("time", time);
    }
}

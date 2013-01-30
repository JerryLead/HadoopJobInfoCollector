package util;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class HtmlFetcher {
	public static Document getHtml(String url) {
		Document doc = null;
		try {
			doc = Jsoup
			.connect(url)
			.ignoreContentType(true) //ignore the content type of this html
			.header("User-Agent",
					"Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1")
			.header("Accept", "text/html,application/xhtml+xml")
			.header("Accept-Language", "zh-cn,zh;q=0.5")
			.header("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7").timeout(10000)
			.get();


		} catch (IOException e) {
			
			try {
				System.out.println(url);
				Thread.sleep(5000);
				doc = Jsoup
						.connect(url)
						.ignoreContentType(true) //ignore the content type of this html
						.header("User-Agent",
								"Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1")
						.header("Accept", "text/html,application/xhtml+xml")
						.header("Accept-Language", "zh-cn,zh;q=0.5")
						.header("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7").timeout(10000)
						.get();
			} catch (IOException e1) {
				
				try {
					Thread.sleep(10000);
					doc = Jsoup
							.connect(url)
							.ignoreContentType(true) //ignore the content type of this html
							.header("User-Agent",
									"Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1")
							.header("Accept", "text/html,application/xhtml+xml")
							.header("Accept-Language", "zh-cn,zh;q=0.5")
							.header("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7").timeout(10000)
							.get();
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (InterruptedException e2) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return doc;
	}

}

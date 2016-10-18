import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.util.concurrent.*;
import java.sql.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

public class Crawler
{
	Connection connection;
	Deque<String> urlQueue;
	Set<String> knownUrls;
	int urlID;
	public Properties props;

	List<String> stopWords = Arrays.asList("a","able","about","across","after","all","almost","also",
		"am","among","an","and","any","are","as","at","be","because","been","but",
		"by","can","cannot","could","dear","did","do","does","either","else","ever",
		"every","for","from","get","got","had","has","have","he","her","hers","him",
		"his","how","however","i","if","in","into","is","it","its","just","least",
		"let","like","likely","may","me","might","most","must","my","neither","no",
		"nor","not","of","off","often","on","only","or","other","our","own","rather",
		"said","say","says","she","should","since","so","some","than","that","the",
		"their","them","then","there","these","they","this","tis","to","too","twas",
		"us","wants","was","we","were","what","when","where","which","while","who",
		"whom","why","will","with","would","yet","you","your");
	List<String> nonWords = Arrays.asList("<",">","::","|","-",")","(","–");

	Crawler() {
		urlID = 0;
		urlQueue = new ConcurrentLinkedDeque<String>();
		knownUrls = new ConcurrentSkipListSet<String>();
	}

	public void readProperties() throws IOException {
		props = new Properties();
		FileInputStream in = new FileInputStream("database.properties");
		props.load(in);
		in.close();
	}

	public void openConnection() throws SQLException, IOException {
		String drivers = props.getProperty("jdbc.drivers");
		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

		String url = props.getProperty("jdbc.url");
		String username = props.getProperty("jdbc.username");
		String password = props.getProperty("jdbc.password");

		connection = DriverManager.getConnection( url, username, password);
	}

	public void createDB() throws SQLException, IOException {
		openConnection();
		Statement stat = connection.createStatement();

		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE URLS");
		} catch (Exception e) {
		}

		// Create the table
		stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");
	}

	public boolean urlFound(String url) throws IOException {
		if (knownUrls.contains(url))
			return true;
		else 
			return false;
	}

	public void insertURLInDB( String url) throws SQLException, IOException {
		Statement stat = connection.createStatement();
		String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','')";
		//System.out.println("Executing "+query);
		stat.executeUpdate( query );
		urlID++;
	}

	public boolean validUrl(String link) {
		if (link.contains("mailto:"))
			return false;
		else if (link.contains("#"))
			return false;
		else if (!link.contains("purdue.edu"))
			return false;
		else if (!link.substring(0, 4).equals("http"))
			return false;
		else if (link.substring(link.length() - 4, link.length()).equals(".PDF"))
			return false;
		else if (link.substring(link.length() - 4, link.length()).equals(".pdf"))
			return false;
		else if (link.substring(link.length() - 4, link.length()).equals(".doc"))
			return false;
		else 
			return true;
	}

	public void parseText(TextNode text) {
		Set<String> words = new HashSet<String>();
		String[] arr = text.text().toLowerCase().split("[ \"\t\"»•.?,–(&):©/;]");
		for (String s: arr) {
			if (stopWords.contains(s) || nonWords.contains(s))
				continue;
			else 
				words.add(s);
		}
		for (String s : words) {
			System.out.print(s + " ");
		}
		System.out.println("TEXT OUTPUT");
		System.out.println(text.text());
	}

	public void fetchURL() {
		try {
			String currentUrl = urlQueue.remove();
			Document doc = Jsoup.connect(currentUrl).get();

			TextNode text = new TextNode(doc.text(), currentUrl);
			parseText(text);
			Elements links = doc.select("a[href]");
			for (Element l : links) {
				String link = l.attr("abs:href");
				// Check it's a valid URL
				if (validUrl(link)) {
					// Check if it is already found
					if (!urlFound(link)) {
						insertURLInDB(link);
						knownUrls.add(link);
						urlQueue.add(link);
						System.out.println(link);
					}
				}
			}
			fetchURL();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Crawler crawler = new Crawler();

		try {
			crawler.readProperties();
			String root = crawler.props.getProperty("crawler.root");
			crawler.createDB();
			crawler.urlQueue.add(root);
			crawler.knownUrls.add(root);
			crawler.fetchURL();

		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}


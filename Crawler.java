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
	Map<String, ArrayList<String>> allWords;
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
	List<String> nonWords = Arrays.asList("<",">","::","|","-",")","(","–"," ","\t");

	Crawler() {
		urlID = 0;
		urlQueue = new ConcurrentLinkedDeque<String>();
		knownUrls = new ConcurrentSkipListSet<String>();
		allWords = new HashMap<String, ArrayList<String>>();
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
		else if (link.substring(link.length() - 4, link.length()).equals(".jpg"))
			return false;
		else 
			return true;
	}

	public void parseText(String text, String url) {
		String[] arr = text.toLowerCase().trim().split("[ \"\t\"»•.?,–&:©/;*]+");
		for (String s: arr) {
			if (stopWords.contains(s) || nonWords.contains(s)) {
				continue;
			} else {
				if (allWords.containsKey(s)) {
					allWords.get(s).add(url);
				} else {
					ArrayList<String> newList = new ArrayList<String>();
					newList.add(url);
					allWords.put(s, newList);
				}
			}
		}
		System.out.println(allWords.size());
	}

	public void fetchURL() {
		try {
			String currentUrl = urlQueue.remove();
			Document doc = Jsoup.connect(currentUrl).get();
			parseText(doc.text(), currentUrl);

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
			while(!crawler.urlQueue.isEmpty()) {
				crawler.fetchURL();
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}


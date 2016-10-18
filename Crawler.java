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

	public boolean urlInDB(String urlFound) throws SQLException, IOException {
		Statement stat = connection.createStatement();
		ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");

		if (result.next()) {
			//System.out.println("URL "+urlFound+" already in DB");
			return true;
		}
		// System.out.println("URL "+urlFound+" not yet in DB");
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
		else if (!link.substring(0, 4).equals("http"))
			return false;
		else if (link.contains("#"))
			return false;
		else if (link.substring(link.length() - 4, link.length()).equals(".pdf"))
			return false;
		else 
			return true;
	}

	public void fetchURL(String urlScanned) {
		try {
			Document doc = Jsoup.connect(urlScanned).get();

			Elements links = doc.select("a[href]");
			for (Element link : links) {
				String linkHref = link.attr("abs:href");
				// Check it's a valid URL
				if (validUrl(linkHref)) {
					// Check if it is already in the database
					if (!urlInDB(linkHref)) {
						insertURLInDB(linkHref);
						System.out.println(linkHref);
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
			crawler.fetchURL(root);
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}


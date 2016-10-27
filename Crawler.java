import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.sql.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

public class Crawler implements Runnable
{
	Connection connection;
	static Deque<String> urlQueue = new ConcurrentLinkedDeque<String>();
	static Set<String> knownUrls = new ConcurrentSkipListSet<String>();
	static Map<String, HashSet<Integer>> allWords = new HashMap<String, HashSet<Integer>>();
	static Map<String,Integer> urlsToIds = new ConcurrentHashMap<String,Integer>();
	static int urlID = 0;
	static int wordID = 0;
	static public Properties props;
	public static int count = 0;
	private final ReentrantLock lock = new ReentrantLock();

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
	List<String> notHtml = Arrays.asList(".png",".jpg",".gif",".doc",".pdf",".PDF","vmdk","cow2",
			"ps.Z",".zip",".JPG",".key","pptx",".xls",".ps",".gz",".tar",".ppt","jpeg","docx",
			".tgz","ppsx",".wrz",".wrl");

	Crawler() {
		try {
			this.connection = openConnection();
		} catch(Exception e) { }
	}

	public static void readProperties() throws IOException {
		props = new Properties();
		FileInputStream in = new FileInputStream("database.properties");
		props.load(in);
		in.close();
	}

	public static Connection openConnection() throws SQLException, IOException {
		String drivers = props.getProperty("jdbc.drivers");
		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

		String url = props.getProperty("jdbc.url");
		String username = props.getProperty("jdbc.username");
		String password = props.getProperty("jdbc.password");

		return DriverManager.getConnection( url, username, password);
	}

	public static void createDB() throws SQLException, IOException {
		Connection c = openConnection();
		Statement stat = c.createStatement();

		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE URLS");
			stat.executeUpdate("DROP TABLE WORDS");
			stat.executeUpdate("DROP TABLE WORDURLS");
		} catch (Exception e) { }

		// Create the table
		stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200), image VARCHAR(512))");
		stat.executeUpdate("CREATE TABLE WORDS (wordid INT, word VARCHAR(32))");
		stat.executeUpdate("CREATE TABLE WORDURLS (wordid INT, urlid INT)");
	}

	public boolean urlFound(String url) throws IOException {
		return knownUrls.contains(url);
	}

	public void insertWordsToUrlsInDB(int wordID, String word) throws SQLException, IOException {
		HashSet<Integer> urls = allWords.get(word);
		PreparedStatement statement = connection.prepareStatement("INSERT INTO wordurls VALUES ( ?, ?)");

		for (Integer i : urls) {
			int in = Integer.valueOf(i);
			statement.setInt(1, wordID);
			statement.setInt(2, in);
			statement.addBatch();
		}
		statement.executeBatch();
	}

	public void insertWordsInDB() throws SQLException, IOException {
		System.out.println("this is supposed to print");
		PreparedStatement statement = connection.prepareStatement("INSERT INTO words VALUES ( ?, ?)");

		for (String s : allWords.keySet()) {
			System.out.println(s);
			statement.setInt(1, wordID);
			statement.setString(2, s);
			statement.addBatch();
			insertWordsToUrlsInDB(wordID, s);
			wordID++;
		}
		statement.executeBatch();
	}

	public void insertURLInDB( String url, String desc, String img) throws SQLException, IOException {
		PreparedStatement statement = connection.prepareStatement("INSERT INTO urls VALUES ( ?, ?, ?, ?)");
		lock.lock();
		try {
			statement.setInt(1, urlID);
			statement.setString(2, url);
			statement.setString(3, desc);
			statement.setString(4, img);
			//System.out.println("Executing "+query);
			statement.executeUpdate();
			urlsToIds.put(url, urlID);
			urlID++;
			knownUrls.add(url);
			if (urlID % 500 == 0) {
				System.out.println(urlID);
				System.out.println(allWords.size());
			}
		} finally {
			lock.unlock();
		}
	}

	public boolean validUrl(String link) {
		if (link.contains("mailto:"))
			return false;
		else if (link.contains("#"))
			return false;
		else if (!link.contains("cs.purdue.edu"))
			return false;
		else if (!link.substring(0, 4).equals("http"))
			return false;
		else if (link.contains("Special:UserLogin&returnto=Main_Page&returntoquery=diff"))
			return false;
		else if (link.contains("weekly_blogging?do=login&sectok="))
			return false;
		else if (notHtml.contains(link.substring(link.length() - 4)))
			return false;
		else if (notHtml.contains(link.substring(link.length() - 3)))
			return false;
		else 
			return true;
	}

	public void parseText(String text, String url) {
		String[] arr = text.toLowerCase().trim().split("[^a-zA-Z0-9'–-]");
		for (String st : arr) {
			if (st.length() == 0 || st.length() == 1) {
				continue;
			}
			String s = (st.length() > 32) ? st.substring(0,32) : st;
			if (stopWords.contains(s) || nonWords.contains(s)) {
				continue;
			} else {
				if (allWords.containsKey(s)) {
					//System.out.println("URL: " + url + " ID: " + urlsToIds.get(url));
					allWords.get(s).add(urlsToIds.get(url));
				} else {
					HashSet<Integer> newList = new HashSet<Integer>();
					newList.add(urlsToIds.get(url));
					allWords.put(s, newList);
				}
			}
		}
	}

	static String getImage(Document doc) {
		String img = "";
		Elements images = doc.select("img");
		for (Element image : images) {
			if (!image.attr("abs:src").equals("https://www.cs.purdue.edu/images/logo.svg")) {
				img = image.attr("abs:src");
				break;
			} else {
				if (img.equals(""))
					img = image.attr("abs:src");
			}
		}
		return img;
	}

	static String getDescription(Document doc) {
		StringBuilder desc = new StringBuilder();
		desc.append(doc.title());
		desc.append(" | ");
		Elements h1 = doc.select("h1");
		for (Element h : h1) {
			if (h.text().trim().equals(""))
				continue;
			desc.append(h.text() + " | ");
			if (desc.length() > 197)
				return desc.toString().substring(0, 197);
		}
		Elements h2 = doc.select("h2");
		for (Element h : h2) {
			if (h.text().trim().equals(""))
				continue;
			desc.append(h.text() + " | ");
			if (desc.length() > 197)
				return desc.toString().substring(0, 197);
		}
		Elements h3 = doc.select("h3");
		for (Element h : h3) {
			if (h.text().trim().equals(""))
				continue;
			desc.append(h.text() + " | ");
			if (desc.length() > 197)
				return desc.toString().substring(0, 197);
		}
		Elements ps = doc.select("p");
		for (Element h : ps) {
			if (h.text().trim().equals(""))
				continue;
			desc.append(h.text() + " | ");
			if (desc.length() > 197)
				return desc.toString().substring(0, 197);
		}
		if (desc.length() > 197)
			return desc.toString().substring(0, 197);
		else
			return desc.toString();
	}

	public void fetchURL() {
		try {
			String currentUrl = urlQueue.remove();
			Document doc = Jsoup.connect(currentUrl).get();

			String description = getDescription(doc) + "...";
			String img = getImage(doc);

			Elements links = doc.select("a[href]");
			for (Element l : links) {
				String link = l.attr("abs:href");
				// Check it's a valid URL
				if (validUrl(link)) {
					// Check if it is already found
					if (!urlFound(link) && knownUrls.size() <= 10000) {
						insertURLInDB(link, description, img);
						parseText(doc.text(), currentUrl);
						knownUrls.add(link);
						urlQueue.add(link);
					}
				}
			}
		} catch (Exception e) { 
				//e.printStackTrace();
		}
	}

	public void run() {
		boolean first = true;
		while (true) {
			if (!urlQueue.isEmpty()) {
				this.fetchURL();
			} else {
				System.out.println("Breaking");
				break;
			}
		}
	}

	public static void main(String[] args) {
		try {
			readProperties();
			String root = props.getProperty("crawler.root");
			createDB();
			urlQueue.add(root);
			knownUrls.add(root);
			urlsToIds.put(root, 0);
		} catch( Exception e) {
			e.printStackTrace();
		}

		Thread threads[] = new Thread[8];
		threads[0] = new Thread(new Crawler());
		threads[0].start();
		try{ Thread.sleep(10000); } catch(Exception e) {}
		for (int i = 1; i < threads.length; i++) {
			threads[i] = new Thread(new Crawler());
			threads[i].start();
		}

		for (int i = 0; i < threads.length; i++) {
			try { threads[i].join(); } catch (Exception e) {
				e.printStackTrace();
			}
		}
		Crawler c = new Crawler();
		try {
			c.insertWordsInDB();
		} catch(Exception e) { 
			e.printStackTrace();
		}
	}
}


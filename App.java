import java.io.*;
import java.util.*;
import java.sql.*;
import java.net.*;
import java.util.regex.*;

public class App extends NanoHTTPD {

	Connection connection;
	static public Properties props;
	static boolean color = true;

	static String bg1 = "<tr style=\"background-color:#BBBBBB;\">";
	static String bg2 = "<tr style=\"background-color:#AAAAAA;\">";
	static String formB = "<form class=\"form-inline\" action='?'><div class=\"form-group\"><input type=\"text\" name='query' placeholder=\"Search\"></div><button type=\"submit\" class=\"btn btn-default\">Go!</button></form>";
	static String form = "<form action='?' method='get'>\n  <p>Search: <input type='text' name='query'></p>\n</form></div>\n";
	static String head = "<html lang=\"en\">" +
		"<head><meta charset=\"utf-8\">" +
		"<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">" + 
		"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" + 
		"<title>Joogle Search Engine</title>" + 
		"<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js\"></script>" + 
		"<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css\">" +
		"<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js\"></script>" +
		"<link rel=\"stylesheet\" type=\"text/css\" href=\"styles/main.css\">" + 
		"</head>"; 
	static String tableStart = "<div class=\"container\">" + 
		"<table class=\"table table-striped\">" + 
		"<thead><tr><th><h3>Results</h3></th></tr></thead>";

	static String tableEnd = "</table></div>";

	public App() throws IOException {
		super(8080);
		start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
		System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
		try {
			this.connection = openConnection();
		} catch(Exception e) { e.printStackTrace(); }
	}

	public String fetchMultiWord(String word) {
		try {
			HashMap<Integer, ArrayList<String>> wordMap1 = new HashMap<Integer, ArrayList<String>>();
			HashMap<Integer, ArrayList<String>> wordMap2 = new HashMap<Integer, ArrayList<String>>();

			String[] words = word.split(" ");
			PreparedStatement stmt = connection.prepareStatement("SELECT urls.urlid, url, description, image, title FROM words LEFT JOIN wordurls ON words.wordid=wordurls.wordid LEFT JOIN urls ON wordurls.urlid=urls.urlid WHERE word=?");
			stmt.setString(1, words[0]);

			ResultSet result = stmt.executeQuery();
			while(result.next()) {
				if (result.getString(2) == null)
					continue;
				ArrayList<String> res = new ArrayList<String>();
				res.add(result.getString(2));
				res.add(result.getString(3));
				res.add(result.getString(4));
				res.add(result.getString(5));
				int urlid = result.getInt(1);
				wordMap1.put(urlid, res);
			}

			PreparedStatement stmt2 = connection.prepareStatement("SELECT urls.urlid, url, description, image, title FROM words LEFT JOIN wordurls ON words.wordid=wordurls.wordid LEFT JOIN urls ON wordurls.urlid=urls.urlid WHERE word=?");
			stmt2.setString(1, words[1]);
			ResultSet result2 = stmt.executeQuery();
			while(result2.next()) {
				if (result2.getString(2) == null)
					continue;
				ArrayList<String> res = new ArrayList<String>();
				res.add(result2.getString(2));
				res.add(result2.getString(3));
				res.add(result2.getString(4));
				res.add(result2.getString(5));
				int urlid = result2.getInt(1);
				wordMap2.put(urlid, res);
			}

			ArrayList<ArrayList<String>> firstToShow = new ArrayList<ArrayList<String>>();
			ArrayList<ArrayList<String>> secondToShow = new ArrayList<ArrayList<String>>();

			for (Integer i : wordMap1.keySet()) {
				if (wordMap2.containsKey(i))
					firstToShow.add(wordMap1.get(i));
				else 
					secondToShow.add(wordMap1.get(i));
			}

			for (Integer i : wordMap2.keySet()) {
				if (!wordMap1.containsKey(i))
					secondToShow.add(wordMap2.get(i));
			}

			StringBuilder res = new StringBuilder(tableStart);

			for (ArrayList<String> l : firstToShow) {
				String row;
				String url = l.get(0);
				String description = l.get(1);
				String image = l.get(2);
				String title = l.get(3);
				if (color) {
					row = bg1 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + "\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				} else {
					row = bg2 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + "\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				}
				res.append(row);
			}

			for (ArrayList<String> l : secondToShow) {
				String row;
				String url = l.get(0);
				String description = l.get(1);
				String image = l.get(2);
				String title = l.get(3);
				if (color) {
					row = bg1 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + "\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				} else {
					row = bg2 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + "\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				}
				res.append(row);
			}

			res.append(tableEnd);
			return res.toString();
		} catch(Exception e) { e.printStackTrace(); }
		return null;
	}

	public String fetchMatchingUrls(String word) {
		try {
			if (word.contains(" ")) 
				return fetchMultiWord(word);

			PreparedStatement stmt = connection.prepareStatement("SELECT url, description, image, title FROM words LEFT JOIN wordurls ON words.wordid=wordurls.wordid LEFT JOIN urls ON wordurls.urlid=urls.urlid WHERE word=?");

			stmt.setString(1, word);
			ResultSet result = stmt.executeQuery();
			StringBuilder res = new StringBuilder(tableStart);
			int i = 0;
			while(result.next()) {
				if (result.getString(1) == null)
					continue;
				String url = result.getString(1);
				String description = result.getString(2);
				String image = result.getString(3);
				String title = result.getString(4);
				String row;
				if (color) {
					row = bg1 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + 
						"\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + 
						url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				} else {
					row = bg2 + "<td><div><a href=\"" + url + "\"><img src=\"" + image + 
						"\" width='128' height='128' alt=\"\" border=3 background=#FFFFFF></img></a></div></td><td width='%80'><a href=\"" + 
						url + "\">" + title + "</a><br><p>" + description + "</p><br><a href=\"" + url + "\">" + url + "</td></tr>";
					color = !color;
				}
				res.append(row);
			}
			res.append(tableEnd);
			return res.toString();
		} catch(Exception e) { e.printStackTrace(); }
		return null;
	}

	public static Connection openConnection() throws SQLException, IOException {
		String drivers = props.getProperty("jdbc.drivers");
		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

		String url = props.getProperty("jdbc.url");
		String username = props.getProperty("jdbc.username");
		String password = props.getProperty("jdbc.password");

		return DriverManager.getConnection( url, username, password);
	}

	public static void readProperties() throws IOException {
		props = new Properties();
		FileInputStream in = new FileInputStream("database.properties");
		props.load(in);
		in.close();
	}

	public static void main(String[] args) {
		try {
			readProperties();
			String root = props.getProperty("crawler.root");
			App a = new App();
		} catch (IOException ioe) {
			System.err.println("Couldn't start server:\n" + ioe);
		}
	}

	@Override
	public Response serve(IHTTPSession session) {
		StringBuilder msg = new StringBuilder();
		msg.append(head + "<body><div class=\"container\"><h1>Joogle Search Engine</h1>\n");
		Map<String, String> parms = session.getParms();
		if (parms.get("query") == null) {
			msg.append(formB);
		} else {
			String word = parms.get("query");
			msg.append(formB);
			msg.append(fetchMatchingUrls(word));
		}
		return newFixedLengthResponse(msg.toString() + "</body></html>\n");
	}
}

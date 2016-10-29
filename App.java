import java.io.*;
import java.util.*;
import java.sql.*;
import java.net.*;
import java.util.regex.*;

public class App extends NanoHTTPD {
	
	Connection connection;
	static public Properties props;

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

	public String fetchMatchingUrls(String word) {
		try {
			PreparedStatement stmt = connection.prepareStatement("SELECT url, description, image, title FROM words LEFT JOIN wordurls ON words.wordid=wordurls.wordid LEFT JOIN urls ON wordurls.urlid=urls.urlid WHERE word=?");
			stmt.setString(1, word);
			ResultSet result = stmt.executeQuery();
			StringBuilder res = new StringBuilder(tableStart);
			while(result.next()) {
				if (result.getString(1) == null)
					continue;
				String url = result.getString(1);
				String description = result.getString(2);
				String image = result.getString(3);
				String title = resutl.getString(4);
				String row = "<tr><td><img src=\"" + image + "\" alt=\"" + url + "\" border=3 height=150 width=150></img></td><td><a href=\"" + url + "\">" + title + "</a><br><p>" + description + "</p></td></tr>";
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
		msg.append(head + "<body><h1>Joogle Search Engine</h1>\n");
		Map<String, String> parms = session.getParms();
		if (parms.get("query") == null) {
			msg.append("<form action='?' method='get'>\n  <p>Search: <input type='text' name='query'></p>\n</form>\n");
		} else {
			String word = parms.get("query");
			msg.append(fetchMatchingUrls(word));
		}
		return newFixedLengthResponse(msg.toString() + "</body></html>\n");
	}
}

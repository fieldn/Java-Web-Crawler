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
			"<link href=\"css/bootstrap.min.css\" rel=\"stylesheet\">" + 
			"</head>"; 

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
			PreparedStatement stmt = connection.prepareStatement("SELECT url, description, image FROM words LEFT JOIN wordurls ON words.wordid=wordurls.wordid LEFT JOIN urls ON wordurls.urlid=urls.urlid WHERE word=?");
			stmt.setString(1, word);
			ResultSet result = stmt.executeQuery();
			StringBuilder res = new StringBuilder();
			while(result.next()) {
				if (result.getString(1) == null)
					continue;
				String url = result.getString(1);
				String description = result.getString(2);
				String image = result.getString(3);
				res.append("<p>" + url + "</p><p>" + description + "</p><p>" + image);
			}
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

target: crawler

crawler: Crawler.java
	javac -cp ./jsoup-1.9.2.jar Crawler.java

app: App.java nanoHTTPD.java
	javac App.java nanoHTTPD.java

clean: 
	rm *.class

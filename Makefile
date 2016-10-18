target: crawler

crawler: *.java
	javac -cp ./jsoup-1.9.2.jar *.java

clean: 
	rm *.class

package com.ksqlDB.Live;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

import java.net.URL;

@SpringBootApplication
public class LiveApplication {

	public static void main(String[] args) throws IOException {

		String query = "SELECT * FROM riderLocations";
		String ksqlDBUrl = "http://localhost:8088";

		// Set up the HTTP connection
		URL url = new URL(ksqlDBUrl + "/query-stream");
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/vnd.ksql.v1+json");
		con.setRequestProperty("Accept", "application/vnd.ksql.v1+json");
		con.setDoOutput(true);

		// Set up the request body
		String requestBody = "{ \"ksql\": \"" + query + "\", \"streamsProperties\": {} }";
		con.getOutputStream().write(requestBody.getBytes());

		// Read the response
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String line;
		while ((line = in.readLine()) != null) {
			System.out.println(line);
		}
		in.close();
	}

}

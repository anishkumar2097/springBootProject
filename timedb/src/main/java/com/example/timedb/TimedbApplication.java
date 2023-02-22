package com.example.timedb;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import java.time.*;
import java.time.temporal.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.influxdb.client.DeleteApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

@SpringBootApplication
public class TimedbApplication {
	// private static String
	// INFLUX_TOKEN="pcQxhjjJnYl69cs7ZqPdhLPEl6AlDT4HDyV_CnloQ1DpzrV6uO_QfY49DFO0jdjyxM-MaZfx2TPGL4f6dfOdhA==";
	private static String ORG = "CGI";

	private static String BUCKET = "timeSeries1";

	private static String token = "u22beltIC2rI5_oXwryFQh8w0scoIn8lYCqGoWmy8sqF5YK4SCWliTZX3gyR9ThpJEmqmK-h5qEqNHdPBfhNUw==";

	private static InfluxDBClient client;

	public static void main(String[] args) {
		SpringApplication.run(TimedbApplication.class, args);

		client = getInfluxDbClient();

		//data insertion synchronously as POJO ,woking successfully
		insertData(client);
		
		
          // fetch the data of all particular date based on ex input=5 days ago
		 // need more work to work successfully
		 getDataByDaysAgo(5);

		
		// data based on particular input date and based on projectId attribute
		 //woking successfully
		getAllDataByParticularDateAndProjectId("2023-02-17T00:00:00.000Z", 4517);

		// getLastValueOfXDaysAgo(5);

		// retrieveDataByRange(client);

		/*
		 * record is basically row in a table
		 */
		// delteRecordByTimeStamp();

		// deleteData(client);

		System.out.println("Hello is the timeDb...2");
		// client.close();
	}

	// start:input date    =   2023-02-17 T00:00:00.000Z
	// stop: input date +1 =   2023-02-18 T00:00:00.000Z
	// start-->date included
	// stop-->date excluded
	// to get all data on particular date ,need start=date,stop=date+1, give all data of "date"

	private static void getAllDataByParticularDateAndProjectId(String date, int id) {

		String incrementDate = Instant.parse(date).plus(Duration.ofDays(1)).toString();

		String start = "time(v:" + date + ")";
		String stop = "time(v:" + incrementDate + ")";

		String fluxQuery = "from(bucket:\"timeSeries1\")\n" 
		+ " |> range(start:" + start + "," + "stop:" + stop + ")\n"
				+ " |> filter(fn: (r) => r[\"_measurement\"] == \"user\")\n"
				+ "  |> filter(fn: (r) => r[\"_field\"] == \"projectId\")\n"
				+ "  |> filter(fn: (r) => r[\"userName\"] == \"Anish Kumar\" or r[\"userName\"] == \"Manish Kumar\")\n"
				+ "  |>filter(fn: (r) => r._value==" + id + ")";

		


		QueryApi queryApi = client.getQueryApi();

		List<FluxTable> userTables = queryApi.query(fluxQuery);
		System.out.println(userTables.size());
	for (FluxTable u : userTables) {
		 System.out.println();
		List<FluxRecord> flxRecord = u.getRecords();
		System.out.println("../....startTime................../_field............/_value............/userName.");
		 for (FluxRecord r : flxRecord) {
	  
		System.out.println(r.getMeasurement()+"..." +r.getTime()+"....."+r.getValueByKey("_field")+"......."+r.getValueByKey("_value")+"..."+r.getValueByKey("userName"));
		 System.out.println();
		 }
		}

	}

	private static void delteRecordByTimeStamp() {
		// TODO Auto-generated method stub

	}

	private static void deleteData(InfluxDBClient client) {

		DeleteApi deleteApi = client.getDeleteApi();
		try {

			OffsetDateTime start = OffsetDateTime.now().minus(1, ChronoUnit.HOURS);
			OffsetDateTime stop = OffsetDateTime.now();

			deleteApi.delete(start, stop, "", "my-bucket", "my-org");

		} catch (InfluxException ie) {
			System.out.println("InfluxException: " + ie);
		}

	}

	/**
	 * option now = () => 2022-01-01T00:00:00Z
	 * 
	 * date.time(t: -1h)// Returns 2021-12-31T23:00:00.000000000Z
	 * 
	 * Return the time for a given relative duration  
	 *  option now = () =>               2023-02-22 05:40:50 GMT+5:30(current time)
	 *   date.time(t: -10d)//    Returns 2023-02-12 05:40:50 GMT+5:30
	 */
	private static void getDataByDaysAgo(int days) {
		
		
		// Return the time for a given relative duration  Using flux date.time() function
		String mdate="date.time(t:-"+days+"d)";
		
		
		//implementing  2023-02-22 05:40:50 GMT+5:30 -->2023-02-22 00:00:00
		 // truncate to ChronoUnit.DAYS
        // means unit smaller than DAY
        // will be Zero   2023-02-22 05:40:50 GMT+5:30 -->2023-02-22 00:00:00
		String date
        = Instant.parse(mdate).truncatedTo(ChronoUnit.DAYS).toString();
        
  

		String incrementDate = Instant.parse(date).plus(Duration.ofDays(1)).toString();

		// Using flux time(v:date) function to convert string to time
		String start = "time(v:" + date + ")";
		String stop = "time(v:" + incrementDate + ")";
				
		String fluxQuery="from(bucket:\"timeSeries1\")\n"
				+ " |> range(start:" + start + "," + "stop:" + stop + ")\n"
				+ " |> filter(fn: (r) => r[\"_measurement\"] == \"user\")\n"
				+ "  |> filter(fn: (r) => r[\"_field\"] == \"projectId\")\n"
				+ "  |> filter(fn: (r) => r[\"userName\"] == \"Anish Kumar\" or r[\"userName\"] == \"Manish Kumar\")\n"
				+ "  |>filter(fn: (r) => r._value==4517)";
		
		QueryApi queryApi = client.getQueryApi();

		List<FluxTable> userTables = queryApi.query(fluxQuery);
		System.out.println(userTables.size());
	for (FluxTable u : userTables) {
		 System.out.println();
		List<FluxRecord> flxRecord = u.getRecords();
		System.out.println("../....startTime................../_field............/_value............/userName.");
		 for (FluxRecord r : flxRecord) {
	  
		System.out.println(r.getMeasurement()+"..." +r.getTime()+"....."+r.getValueByKey("_field")+"......."+r.getValueByKey("_value")+"..."+r.getValueByKey("userName"));
		 System.out.println();
		 }
		}

	}

	/**
	 * For querying data we use QueryApi that allow perform synchronous,
	 * asynchronous and also use raw query response.
	 * 
	 * For POJO mapping, snake_case column names are mapped to camelCase field names
	 * if exact matches not found.
	 */


	// get InfluxDbClient here
	public static InfluxDBClient getInfluxDbClient() {
		// String token =
		// "u22beltIC2rI5_oXwryFQh8w0scoIn8lYCqGoWmy8sqF5YK4SCWliTZX3gyR9ThpJEmqmK-h5qEqNHdPBfhNUw==";

		InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray(), ORG);

		return client;
	}

	public static void insertData(InfluxDBClient client) {

		User user = new User();
		user.setProjectId(4517);
		user.setTime(Instant.now());
		user.setUserEmail("manishKumaar2096@gmail.com");
		user.setUserId(117);
		user.setUserName("Manish Kumar");
		WriteApiBlocking writeApi = client.getWriteApiBlocking();

		writeApi.writeMeasurement(BUCKET, ORG, WritePrecision.NS, user);

	}
//last value can be fetched by flux last() function if we get the table based on days ago
	private static void getLastValueOfXDaysAgo(int X) {

	}
}

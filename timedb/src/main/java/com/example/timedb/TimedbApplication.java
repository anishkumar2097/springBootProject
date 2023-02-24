package com.example.timedb;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
	
	
	/*
	 * 
	 * 
	 * table     _measurement            _field              _value          _start                          _stop                                   _time                           userName

 	    0	      user                  projectId           4563          2023-01-24T13:00:19.298Z	       2023-02-23T13:00:19.298Z	              2023-02-15T09:29:11.694Z	        Anish Kumar
            0         user		    projectId           4563          2023-01-24T13:00:19.298Z	       2023-02-23T13:00:19.298Z	               2023-02-15T09:34:49.18           Anish kumar
	 * 
	 * 
	 * 
	 * Result --> series of "tables" with only single _field and a single _value column
	 * 
	 * 
	 * The _measurement column shows the name of the measurement 
	 * 
	 * A field includes a field key stored in the _field column and a field value stored in the _value column.
	 * 
	 * 
	 * 
	 * Tags are indexed: Tags are optional. You don’t need tags in your data structure, but it’s typically a good idea to include tags.
	 *  Because tags are indexed, queries on tags are faster than queries on fields. This makes tags ideal for storing commonly-queried metadata.
	 * 
	 * 
	 * Timestamp-All data stored in InfluxDB has a _time column that stores timestamp (_time) column
	 * 
	 */
	// private static String
	// INFLUX_TOKEN="pcQxhjjJnYl69cs7ZqPdhLPEl6AlDT4HDyV_CnloQ1DpzrV6uO_QfY49DFO0jdjyxM-MaZfx2TPGL4f6dfOdhA==";
	private static String ORG = "CGI";

	private static String BUCKET ="timeSeries1";

	private static String token = "u22beltIC2rI5_oXwryFQh8w0scoIn8lYCqGoWmy8sqF5YK4SCWliTZX3gyR9ThpJEmqmK-h5qEqNHdPBfhNUw==";

	private static InfluxDBClient client;
	
	private static String  MEASUREMENT="user";
	

	public static void main(String[] args) {
		SpringApplication.run(TimedbApplication.class, args);
                  client = getInfluxDbClient();
		
		

		// data insertion synchronously as POJO ,
		// woking successfully
		   insertData(client);

		// fetch all date of past X minues ago
      
		getAllDataByPastMinutes("30"); 
		   
		// fetch the data of all particular date based on ex input=6 days ago
		
	  	getDataByDaysAgo(7);

		// data based on particular input date and based on projectId attribute(_field)
		// woking successfully
		
		 getAllDataByParticularDate("2023-02-17T00:00:00.000Z", "projectId");
		 
		 
          		

		 // getLastValueOfXDaysAgo(5);

		// retrieveDataByRange(client);
       
		  
		/*
		
		 * 
		 * **InfluxDB 2.6 ** supports deleting data by any column or tag except the following:
                 *
                 *   _time
                 *  _field
                 *   _value
                 *    InfluxDB 2.6 does not support deleting data by field.
		 * 
		 * 
		 */
		// delteRecordByTimeStamp();

		// deleteAllData();

		System.out.println("Hello is the timeDb...2");
		// client.close();
		
		

		
			
	}
	
	
	private static void getAllDataByPastMinutes(String minute) {
		
		//String start = "time(v:" + minute + ")";
		
		String fluxQuery="from(bucket: \"timeSeries1\")\n"
				+ "  |> range(start: "+"-"+minute+"m"+","+" stop:now())\n"
				+ "  |> filter(fn: (r) => r[\"_measurement\"] == \"user\")";
		
		QueryApi queryApi = client.getQueryApi();

		List<FluxTable> userTables = queryApi.query(fluxQuery);
		System.out.println(userTables.size());
		for (FluxTable u : userTables) {
			System.out.println();
			List<FluxRecord> flxRecord = u.getRecords();
			System.out.println("../....startTime................../_field............/_value............/userName.");
			for (FluxRecord r : flxRecord) {

				System.out.println(r.getMeasurement() + "..." + r.getTime() + "....." + r.getValueByKey("_field")
						+ "......." + r.getValueByKey("_value") + "..." + r.getValueByKey("userName"));
				System.out.println();
			}
		}
		
	}


	// start:input date = 2023-02-17 T00:00:00.000Z
	// stop: input date +1 = 2023-02-18 T00:00:00.000Z
	// start-->date included
	// stop-->date excluded
	// to get all data on particular date ,need start=date,stop=date+1, give all
	// data of "date",not of date+1

	private static void getAllDataByParticularDate(String date,String field ) {

		System.out.println(date);

		String incrementDate = getIncrementDateByOneDay(date);

		System.out.println(incrementDate);

		String start = "time(v:" + date + ")";
		String stop = "time(v:" + incrementDate + ")";

		//String fluxQuery = "from(bucket:"+ BUCKET+")\n"
		 //       + " |> range(start:" + start + "," + "stop:" + stop + ")\n"
			//	+ " |> filter(fn: (r) => r[\"_measurement\"] =="+MEASUREMENT+")n"
		//		+  "|> filter(fn: (r) => r[\"_field\"] == "+field+")";

		
		String fluxQuery="from(bucket: \"timeSeries1\")\n"
				 + " |> range(start:" + start + "," + "stop:" + stop + ")\n"
				+ "  |> filter(fn: (r) => r[\"_measurement\"] == \"user\")\n"
				+ "  |> filter(fn: (r) => r[\"_field\"] == \"projectId\")\n"
				+ "  |> filter(fn: (r) => r[\"userName\"] == \"Manish Kumar\")";
		QueryApi queryApi = client.getQueryApi();

		List<FluxTable> userTables = queryApi.query(fluxQuery);
		System.out.println(userTables.size());
		for (FluxTable u : userTables) {
			System.out.println();
			List<FluxRecord> flxRecord = u.getRecords();
			System.out.println("../....startTime................../_field............/_value............/userName.");
			for (FluxRecord r : flxRecord) {

				System.out.println(r.getMeasurement() + "..." + r.getTime() + "....." + r.getValueByKey("_field")
						+ "......." + r.getValueByKey("_value") + "..." + r.getValueByKey("userName"));
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
	 * start date: inputDate stop:incrementDate
	 * 
	 */
	private static void getDataByDaysAgo(int days) {

		String date = getDateOfDaysAgo(days);

		String incrementDate = getIncrementDateByOneDay(date);
 
		System.out.println(incrementDate);

		

		
         getData(date,incrementDate);

	}

	private static void getData(String date, String incrementDate ) {
		
		String start = "time(v:" + date + ")";
		String stop = "time(v:" + incrementDate + ")";
	
		// 
		//String fluxQuery =  "from(bucket:"+ BUCKET+")\n"
		//   + " |> range(start:" + start + "," + "stop:" + stop + ")\n"
		//	+ " |> filter(fn: (r) => r[\"_measurement\"] =="+MEASUREMENT+")";
		String fluxQuery="from(bucket: \"timeSeries1\")\n"
				 + " |> range(start:" + start + "," + "stop:" + stop + ")\n"
				+ "  |> filter(fn: (r) => r[\"_measurement\"] == \"user\")";

		QueryApi queryApi = client.getQueryApi();

		List<FluxTable> userTables = queryApi.query(fluxQuery);
		System.out.println(userTables.size());
		for (FluxTable u : userTables) {
			System.out.println();
			List<FluxRecord> flxRecord = u.getRecords();
			System.out.println("../....startTime................../_field............/_value............/userName.");
			for (FluxRecord r : flxRecord) {

				System.out.println(r.getMeasurement() + "..." + r.getTime() + "....." + r.getValueByKey("_field")
						+ "......." + r.getValueByKey("_value") + "..." + r.getValueByKey("userName"));
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

		getDateOfDaysAgo(X);
		
		// need to complete

	}

	private static String getDateOfDaysAgo(int days) {
		// TODO Auto-generated method stub
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE, -days);

		return dateFormat.format(c.getTime());
	}

	private static String getIncrementDateByOneDay(String date) {
		// TODO Auto-generated method stub
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();

		try {
			c.setTime(dateFormat.parse(date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		c.add(Calendar.DATE, 1);

		return dateFormat.format(c.getTime());
	}
}

package com.example.timedb;

import java.time.Instant;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;

@Measurement(name="temp")
public class Temperature {
	
	  @Column(tag = true)
	  String host;
	
	  @Column
	  String t;
	  
	  @Column(timestamp = true)
	  Instant time;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getT() {
		return t;
	}

	public void setT(String t) {
		this.t = t;
	}

	public Instant getTime() {
		return time;
	}

	public void setTime(Instant time) {
		this.time = time;
	}
	  
	  

}

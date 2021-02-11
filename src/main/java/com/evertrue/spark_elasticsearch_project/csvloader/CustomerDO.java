package com.evertrue.spark_elasticsearch_project.csvloader;

import java.io.Serializable;

public class CustomerDO implements Serializable {

	
	private static final long serialVersionUID = 1L;
	private int id;
	private String first_name;
	private String last_name;
	private String state;
	private int age;


	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	
	public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	
	public String getLast_name() {
		return last_name;
	}

	
	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}


	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}


	public int getAge() {
		return age;
	}

	
	public void setAge(int age) {
		this.age = age;
	}

	public CustomerDO() {
		// TODO Auto-generated constructor stub
	}

	public CustomerDO(int id, String first_name, String last_name, int age, String state) {
		setId(id);
		setFirst_name(first_name);
		setLast_name(last_name);
		setState(state);
		setAge(age);
	}

}

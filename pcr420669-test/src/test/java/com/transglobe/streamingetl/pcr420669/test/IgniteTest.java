package com.transglobe.streamingetl.pcr420669.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteTest {

	public static void main(String[] args) {
		test1();
	}
	
	private static void test1() {
		IgniteConfiguration cfg1 = new IgniteConfiguration();

		cfg1.setGridName("name1");

		Ignite ignite1 = Ignition.start(cfg1);

		IgniteConfiguration cfg2 = new IgniteConfiguration();

		cfg2.setGridName("name2");

		Ignite ignite2 = Ignition.start(cfg2);
	}

}

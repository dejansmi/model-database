
package com.dejans.ModelDatabase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class ModelDatabaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(ModelDatabaseApplication.class, args);
		ModelDatabaseCommands dbc = new ModelDatabaseCommands(args);
		System.exit(0);
	}
}

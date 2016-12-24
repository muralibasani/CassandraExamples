package com.test.cassandraexamples;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class GettingStarted {

	public static void main(String[] args) {

		Cluster cluster;
		Session session;
		ResultSet results;
		Row rows;
                String keyspace = "testkeyspace";
		// Connect to the cluster and keyspace "demo"
		cluster = Cluster
				.builder()
				.addContactPoint("ec2-35-165-129-69.us-west-2.compute.amazonaws.com")
                                .withPort(19042)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.build();
		session = cluster.connect(keyspace);

		// Insert one record into the users table
		PreparedStatement statement = session.prepare(

		"INSERT INTO users" + "(lastname, age, city, email, firstname)"
				+ "VALUES (?,?,?,?,?);");

		BoundStatement boundStatement = new BoundStatement(statement);

		session.execute(boundStatement.bind("Jones", 35, "Austin",
				"bob@example.com", "Bob"));

		// Use select to get the user we just entered
		Statement select = QueryBuilder.select().all().from(keyspace, "users")
				.where(eq("lastname", "Jones"));
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"),
					row.getInt("age"));
		}

		// Update the same user with a new age
		Statement update = QueryBuilder.update(keyspace, "users")
				.with(QueryBuilder.set("age", 36))
				.where((QueryBuilder.eq("lastname", "Jones")));
		session.execute(update);
		
		// Select and show the change
		select = QueryBuilder.select().all().from(keyspace, "users")
				.where(eq("lastname", "Jones"));
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"),
					row.getInt("age"));
		}

		// Delete the user from the users table
		Statement delete = QueryBuilder.delete().from("users")
				.where(QueryBuilder.eq("lastname", "Jones"));
		results = session.execute(delete);


		// Show that the user is gone
                select = QueryBuilder.select().all().from(keyspace, "users");
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d %s %s %s\n", row.getString("lastname"),
					row.getInt("age"), row.getString("city"),
					row.getString("email"), row.getString("firstname"));
		}

		// Clean up the connection by closing it
		cluster.close();
	}
}


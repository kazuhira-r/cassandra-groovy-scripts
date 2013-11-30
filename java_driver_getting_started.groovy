@Grab('com.datastax.cassandra:cassandra-driver-core:1.0.0')
@Grab('org.apache.cassandra:cassandra-all:1.2.5')
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.Metadata

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.PreparedStatement

def cluster = Cluster.builder().addContactPoint('localhost').withPort(9042).build()

try {
    def metadata = cluster.getMetadata()
    println("Connected to cluster: ${metadata.clusterName}")

    for (host in metadata.allHosts) {
        println("Datacenter: $host.datacenter, Host: $host.address, Rack: $host.rack")
    }

    def session = cluster.connect('cqldemo')

    println('----- Execute Statement -----')
    for (row in session.execute('SELECT * FROM Books')) {
        println("isbn13: ${row.getString('isbn13')}, " +
                    "price: ${row.getInt('price')}, " + 
                    "publish_date: ${row.getDate('publish_date')}, " +
                    "title: ${row.getString('title')}")
        /*
        println("isbn13: ${row.getString(0)}, " +
                    "price: ${row.getInt(1)}, " + 
                    "publish_date: ${row.getDate(2)}, " +
                    "title: ${row.getString(3)}")
        */
    }

    def preparedStatement = session.prepare('SELECT * FROM Books WHERE isbn13 = ?')
    def boundStatement = new BoundStatement(preparedStatement)

    println('----- Execute PreparedStatement -----')
    for (row in session.execute(boundStatement.bind('978-4798128436'))) {
    // for (row in session.execute(boundStatement.setString(0, '978-4798128436'))) {
        println("isbn13: ${row.getString('isbn13')}, " +
                    "price: ${row.getInt('price')}, " + 
                    "publish_date: ${row.getDate('publish_date')}, " +
                    "title: ${row.getString('title')}")
    }
} finally {
    cluster.shutdown()
}

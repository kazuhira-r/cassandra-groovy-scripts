@GrabConfig(systemClassLoader = true)
@Grab('org.apache.cassandra:cassandra-all:1.2.4')
@Grab('org.apache-extras.cassandra-jdbc:cassandra-jdbc:1.2.1')
import java.sql.DriverManager

Class.forName('org.apache.cassandra.cql.jdbc.CassandraDriver')
def conn = DriverManager.getConnection('jdbc:cassandra://localhost:9160/cqldemo')

try {
    def ps =
        conn.prepareStatement('SELECT isbn13, price, publish_date, title FROM books WHERE isbn13 = ?')
    ps.setString(1, '978-4798128436')

    def rs = ps.executeQuery()
    while (rs.next()) {
        def isbn13 = rs.getString('isbn13')  // rs.getString(1)とかでもOK
        def price = rs.getInt('price')
        def publishDate = rs.getTimestamp('publish_date')
        def title = rs.getString('title')

        println("isbn13 = $isbn13, price = $price, publishDate = $publishDate, title = $title")
    }
} finally {
    conn.close()
}



import groovy.sql.Sql

Sql.withInstance('jdbc:cassandra://localhost:9160/cqldemo',
                 'org.apache.cassandra.cql.jdbc.CassandraDriver') { sql ->
    sql.eachRow('SELECT isbn13, price, publish_date, title FROM books',
                { println("row -> $it") })

    try {
        sql.eachRow('SELECT isbn13, price, publish_date, title FROM books WHERE isbn13 = ?',
                    ['978-4873115290'],
                    { println("row -> $it") })
    } catch (e) {
        e.printStackTrace()
        // 5 06, 2013 8:27:27 午後 groovy.sql.Sql eachRow
        // WARNING: Failed to execute: SELECT isbn13, price, publish_date, title FROM books WHERE isbn13 = ? because: the Cassandra implementation does not support this method
        // java.sql.SQLFeatureNotSupportedException: the Cassandra implementation does not support this method
        //     at org.apache.cassandra.cql.jdbc.CassandraConnection.prepareStatement(CassandraConnection.java:365)
        //     at groovy.sql.Sql$CreatePreparedStatementCommand.execute(Sql.java:3996)
        //     at groovy.sql.Sql$CreatePreparedStatementCommand.execute(Sql.java:3978)
        //     at groovy.sql.Sql.getAbstractStatement(Sql.java:3858)
        //     at groovy.sql.Sql.getPreparedStatement(Sql.java:3873)
        //     at groovy.sql.Sql.getPreparedStatement(Sql.java:3921)
        //     at groovy.sql.Sql.eachRow(Sql.java:1241)
        //     at groovy.sql.Sql.eachRow(Sql.java:1329)
        //     at groovy.sql.Sql.eachRow(Sql.java:1383)
    }
}

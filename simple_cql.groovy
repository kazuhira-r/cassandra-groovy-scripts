import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

@Grab('org.apache.cassandra:cassandra-all:1.2.4')
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.Compression
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.CqlResult
import org.apache.cassandra.thrift.CqlRow

import org.apache.cassandra.db.marshal.DateType
import org.apache.cassandra.db.marshal.Int32Type
import org.apache.cassandra.db.marshal.LongType
import org.apache.cassandra.db.marshal.UTF8Type

import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol

def charset = StandardCharsets.UTF_8

def transport = new TFramedTransport(new TSocket('localhost', 9160))
def protocol = new TBinaryProtocol(transport)
def client = new Cassandra.Client(protocol)

transport.open()
client.set_keyspace('cqldemo')

try {
    println('Execute [SELECT * FROM books]')

    CqlResult cqlResult = 
        client.execute_cql3_query(ByteBuffer.wrap('SELECT * FROM books'.getBytes(charset)),
                                  Compression.NONE,
                                  ConsistencyLevel.ONE)

    println('Type => ' + cqlResult.getType())
    println('Rows Size => ' + cqlResult.getRowsSize())
    println('Schema => ' + cqlResult.getSchema())

    def valueTypeMap = cqlResult.getSchema().getValue_types()
    def marshaller = { name, binary ->
        def typeClass = Cassandra.class.getClassLoader().loadClass(name)
        def field = typeClass.getField('instance')
        field.get(null).compose(ByteBuffer.wrap(binary))
    }

    def namedMarshaller =
        [isbn13: { binary -> UTF8Type.instance.compose(ByteBuffer.wrap(binary)) },
         title: { binary -> UTF8Type.instance.compose(ByteBuffer.wrap(binary)) },
         price: { binary -> Int32Type.instance.compose(ByteBuffer.wrap(binary)) },
         publish_date: { binary -> DateType.instance.compose(ByteBuffer.wrap(binary)).format('yyyy/MM/dd') }]

    /*
    def factories =
        [isbn13: { binary -> charset.decode(ByteBuffer.wrap(binary)).toString() },
         title: { binary -> charset.decode(ByteBuffer.wrap(binary)).toString() },
         price: { binary -> ByteBuffer.wrap(binary).getInt() },
         publish_date: { binary -> new Date(ByteBuffer.wrap(binary).getLong()).format('yyyy/MM/dd') }]
    */

    for (CqlRow row : cqlResult.getRows()) {
        println("Key => " + new String(row.getKey(), charset))

        for (Column column : row.getColumns()) {
            def name = new String(column.getName(), charset)
            println("[name: value] => [" +
                        name +
                        ': ' +
                        namedMarshaller[name](column.getValue()) +
                        // marshaller(valueTypeMap[ByteBuffer.wrap(column.getName())], column.getValue()) +
                        // factories[name](column.getValue()) +
                        ']')
        }
    }

    println('--------------------------------------------------------')

    println('Execute [SELECT COUNT(*) FROM books]')

    cqlResult = client.execute_cql3_query(ByteBuffer.wrap('SELECT COUNT(*) FROM books'.getBytes(charset)),
                                          Compression.NONE,
                                          ConsistencyLevel.ONE)

    println("Type => " + cqlResult.getType())
    println('Rows Size => ' + cqlResult.getRowsSize())
    println('Schema => ' + cqlResult.getSchema())

    factories = [count: { binary -> ByteBuffer.wrap(binary).getInt() }]

    for (CqlRow row : cqlResult.getRows()) {
        println("Key => " + new String(row.getKey(), charset))

        for (Column column : row.getColumns()) {
            def name = new String(column.getName(), charset)
            println("[name: value] => [" +
                        name +
                        ': ' +
                        LongType.instance.compose(ByteBuffer.wrap(column.getValue())) +
                        ']')
        }
    }
} finally {
    transport.close()
}

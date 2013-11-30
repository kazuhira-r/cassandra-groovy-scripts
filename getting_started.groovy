import java.nio.ByteBuffer

//@Grab('org.apache.cassandra:cassandra-thrift:1.2.4')
@Grab('org.apache.cassandra:cassandra-all:1.2.4')
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol

def transport = new TFramedTransport(new TSocket('localhost', 9160))
def protocol = new TBinaryProtocol(transport)

def client = new Cassandra.Client(protocol)

transport.open()

try {
    def keyspaceName = 'Bookshelf'
    def columnFamilyName = 'Books'

    client.set_keyspace(keyspaceName)

    def timestamp = System.currentTimeMillis()

    // 1冊目登録
    def id1 = '1'

    def isbnColumn1 = new Column(ByteBuffer.wrap('isbn'.getBytes('UTF-8')))
    isbnColumn1.setValue(ByteBuffer.wrap('978-4873115290'.getBytes('UTF-8')))
    isbnColumn1.setTimestamp(timestamp)

    def nameColumn1 = new Column(ByteBuffer.wrap('name'.getBytes('UTF-8')))
    nameColumn1.setValue(ByteBuffer.wrap('Cassandra'.getBytes('UTF-8')))
    nameColumn1.setTimestamp(timestamp)

    def priceColumn1 = new Column(ByteBuffer.wrap('price'.getBytes('UTF-8')))
    priceColumn1.setValue(ByteBuffer.wrap('3570'.getBytes('UTF-8')))
    priceColumn1.setTimestamp(timestamp)

    def columnParent = new ColumnParent(columnFamilyName)

    client.insert(ByteBuffer.wrap(id1.getBytes('UTF-8')),
                  columnParent,
                  isbnColumn1,
                  ConsistencyLevel.ALL)
    client.insert(ByteBuffer.wrap(id1.getBytes('UTF-8')),
                  columnParent,
                  nameColumn1,
                  ConsistencyLevel.ALL)
    client.insert(ByteBuffer.wrap(id1.getBytes('UTF-8')),
                  columnParent,
                  priceColumn1,
                  ConsistencyLevel.ALL)

    // 2冊目登録
    def id2 = '2'

    def isbnColumn2 = new Column(ByteBuffer.wrap('isbn'.getBytes('UTF-8')))
    isbnColumn2.setValue(ByteBuffer.wrap('978-4798128436'.getBytes('UTF-8')))
    isbnColumn2.setTimestamp(timestamp)

    def nameColumn2 = new Column(ByteBuffer.wrap('name'.getBytes('UTF-8')))
    nameColumn2.setValue(ByteBuffer.wrap('Cassandra実用システムインテグレーション'.getBytes('UTF-8')))
    nameColumn2.setTimestamp(timestamp)

    def priceColumn2 = new Column(ByteBuffer.wrap('price'.getBytes('UTF-8')))
    priceColumn2.setValue(ByteBuffer.wrap('3360'.getBytes('UTF-8')))
    priceColumn2.setTimestamp(timestamp)

    def columnParent2 = new ColumnParent(columnFamilyName)

    client.insert(ByteBuffer.wrap(id2.getBytes('UTF-8')),
                  columnParent,
                  isbnColumn2,
                  ConsistencyLevel.ALL)
    client.insert(ByteBuffer.wrap(id2.getBytes('UTF-8')),
                  columnParent,
                  nameColumn2,
                  ConsistencyLevel.ALL)
    client.insert(ByteBuffer.wrap(id2.getBytes('UTF-8')),
                  columnParent,
                  priceColumn2,
                  ConsistencyLevel.ALL)

    // カラムを、キー指定で取得
    def predicate = new SlicePredicate()
    predicate.setSlice_range(
                             new SliceRange(
                                            ByteBuffer.wrap(new byte[0]),
                                            ByteBuffer.wrap(new byte[0]),
                                            false,
                                            100))

    def columnByKey1 =
        client.get_slice(ByteBuffer.wrap(id1.getBytes()),
                         columnParent,
                         predicate,
                         ConsistencyLevel.ALL);
    println("columnByKey1 => " + columnByKey1)

    def columnByKey2 =
        client.get_slice(ByteBuffer.wrap(id2.getBytes()),
                         columnParent,
                         predicate,
                         ConsistencyLevel.ALL);
    println("columnByKey2 => " + columnByKey2)

    // 全キーを取得
    def keyRange = new KeyRange(100)
    keyRange.setStart_key(new byte[0])
    keyRange.setEnd_key(new byte[0])
    def keySlices = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE)
    println("size => " + keySlices.size())
    println("keySlices => " + keySlices)

    for (KeySlice ks : keySlices) {
        println("keySlice => " + new String(ks.getKey()))
    }
} finally {
    transport.close()
}

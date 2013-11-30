import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

@Grab('org.apache.cassandra:cassandra-all:1.2.4')
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ColumnPath
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.Deletion
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.Mutation
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol

String.metaClass {
    asBinary << { getBytes(StandardCharsets.UTF_8) }
    asBuffer << { ByteBuffer.wrap(asBinary()) }
}

Cassandra.Client.metaClass {
    'static' {
        openWith << { conn, cls ->
            def transport = new TFramedTransport(new TSocket(conn['host'], conn['port']))
            def protocol = new TBinaryProtocol(transport)
            def client = new Cassandra.Client(protocol)

            transport.open()

            try {
                client.set_keyspace(conn['keyspace'])
                cls(client)
            } finally {
                transport.close()
            }
        }
    }
}

Column.metaClass.static.create << { name, value, timestamp ->
    def column = new Column(name.toString().asBuffer())
    column.setValue(value.toString().asBuffer())
    column.timestamp = timestamp
    column
}

Mutation.metaClass {
    'static' {
        columnsMap << { key, columnFamilyName, columns ->
            def mutations = []
            for (column in columns) {
                def colOrSuper = new ColumnOrSuperColumn().setColumn(column)
                def mutation = new Mutation()
                mutation.column_or_supercolumn = colOrSuper
                mutations << mutation
            }
            def map = [: ]
            def mutationMap = [: ]
            mutationMap[columnFamilyName] = mutations
            map[key.asBuffer()] = mutationMap
            map
        }

        deleteColumnsMap << { key, columnFamilyName, columnNames, timestamp ->
            def predicate = new SlicePredicate().setColumn_names(columnNames.collect { it.asBuffer() })
            def deletion = new Deletion()
            deletion.predicate = predicate
            deletion.timestamp = timestamp

            def mutation = new Mutation()
            mutation.deletion = deletion

            def map = [: ]
            def mutationMap = [: ]
            mutationMap[columnFamilyName] = [mutation]
            map[key.asBuffer()] = mutationMap
            map
        }
    }
}

Cassandra.Client.openWith([host: 'localhost',
                           port: 9160,
                           keyspace: 'Room']) { client ->
    def columnFamilyName = 'Users'
    def columnParent = new ColumnParent(columnFamilyName)

    def timestamp = System.currentTimeMillis() * 1000

    // カラムをひとつずつ登録
    client.insert('1'.asBuffer(),
                  columnParent,
                  Column.create('name', 'Suzuki Taro', timestamp),
                  ConsistencyLevel.ALL)
    client.insert('1'.asBuffer(),
                  columnParent,
                  Column.create('age', 20, timestamp),
                  ConsistencyLevel.ALL)
    client.insert('1'.asBuffer(),
                  columnParent,
                  Column.create('occupation', 'System Engineer', timestamp),
                  ConsistencyLevel.ALL)

    // 複数カラムの一括登録
    client.batch_mutate(Mutation.columnsMap('2',
                                            columnFamilyName,
                                            [Column.create('name', 'Tanaka Jiro', timestamp),
                                             Column.create('age', 22, timestamp),
                                             Column.create('occupation', 'Programmer', timestamp)]),
                        ConsistencyLevel.ALL)

    // 単一カラムを取得
    def targetColumnPath = new ColumnPath(columnFamilyName)
    targetColumnPath.setColumn('name'.asBuffer())
    def colOrSupCol = client.get('1'.asBuffer(),
                               targetColumnPath,
                               ConsistencyLevel.ALL)
    colOrSupCol.column.with {
        println('get one coloumn: [name: value] => [' +
                    new String(name, StandardCharsets.UTF_8) + ': ' +
                    new String(value, StandardCharsets.UTF_8) + ']')
    }

    // 複数カラムを一括取得
    def predicate = new SlicePredicate()
    def range = new SliceRange('name'.asBuffer(), //ByteBuffer.wrap(new byte[0]),  // Start
                               'occupation'.asBuffer(), //ByteBuffer.wrap(new byte[0]),  // Finish
                               false,  // Reverse
                               100) // Count
    predicate.slice_range = range
    def colomnOrSuperColumns = client.get_slice('1'.asBuffer(),
                                                columnParent,
                                                predicate,
                                                ConsistencyLevel.ALL)
    for (columnOrSuperColumn in colomnOrSuperColumns) {
        def column = columnOrSuperColumn.column
        println('get multi column: [name: value] => [' +
                    new String(column.name, StandardCharsets.UTF_8) + ': ' +
                    new String(column.value, StandardCharsets.UTF_8) + ']')
    }

    // カラム数の取得
    def predicateForCount = new SlicePredicate()
    def rangeForCount = new SliceRange(ByteBuffer.wrap(new byte[0]),
                                       ByteBuffer.wrap(new byte[0]),
                                       false,
                                       100)
    predicateForCount.slice_range = rangeForCount
    def count = client.get_count('1'.asBuffer(),
                                 columnParent,
                                 predicateForCount,
                                 ConsistencyLevel.ALL)
    println("column count => $count")

    // 特定のカラムの削除
    client.remove('1'.asBuffer(),
                  new ColumnPath(columnFamilyName).setColumn('occupation'.asBuffer()),
                  System.currentTimeMillis() * 1000,
                  ConsistencyLevel.ALL)

    // 名前を指定して、複数カラムの一括削除
    // が、ロウは残る…
    /*
    client.batch_mutate(Mutation.deleteColumnsMap('2',
                                                  columnFamilyName,
                                                  ['name', 'age', 'occupation'],
                                                  System.currentTimeMillis() * 1000),
                        ConsistencyLevel.ALL)
    */

    // 複数カラムの一括削除
    // ＊これでカラム全部が消える
    // が、やっぱりロウは残る…
    /*
    client.remove('2'.asBuffer(),
                  new ColumnPath(columnFamilyName),
                  System.currentTimeMillis() * 1000,
                  ConsistencyLevel.ALL)
    */
    // ＊batch_mutateでは、名前の範囲指定による一括削除は1.2.4でも不可
    // Caught: InvalidRequestException(why:Deletion does not yet support SliceRange predicates.)
    // InvalidRequestException(why:Deletion does not yet support SliceRange predicates.)


    // カラムファミリーの全データ削除
    // client.truncate(columnFamilyName)
}

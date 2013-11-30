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
import org.apache.cassandra.thrift.IndexClause
import org.apache.cassandra.thrift.IndexExpression
import org.apache.cassandra.thrift.IndexOperator
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
    }
}

Cassandra.Client.openWith([host: 'localhost',
                           port: 9160,
                           keyspace: 'Room']) { client ->
    def columnFamilyName = 'Users'
    def columnParent = new ColumnParent(columnFamilyName)
    def timestamp = System.currentTimeMillis() * 1000

    // データ登録
    client.batch_mutate(Mutation.columnsMap('1',
                                            columnFamilyName,
                                            [Column.create('name', 'Suzuki Taro', timestamp),
                                             Column.create('age', 20, timestamp),
                                             Column.create('occupation', 'System Engineer', timestamp)]),
                        ConsistencyLevel.ALL)

    client.batch_mutate(Mutation.columnsMap('2',
                                            columnFamilyName,
                                            [Column.create('name', 'Tanaka Jiro', timestamp),
                                             Column.create('age', 22, timestamp),
                                             Column.create('occupation', 'Programmer', timestamp)]),
                        ConsistencyLevel.ALL)

    client.batch_mutate(Mutation.columnsMap('3',
                                            columnFamilyName,
                                            [Column.create('name', 'Nakata Saburo', timestamp),
                                             Column.create('age', 27, timestamp),
                                             Column.create('occupation', 'Sales Engineer', timestamp)]),
                        ConsistencyLevel.ALL)

    // multiget_sliceを使って、複数ロウを取得
    def multiGetSlicePredicate = new SlicePredicate()
    multiGetSlicePredicate.slice_range = new SliceRange(ByteBuffer.wrap(new byte[0]),
                                                        ByteBuffer.wrap(new byte[0]),
                                                        false,
                                                        100)

    def multiGetSliceMap =
        client.multiget_slice(['1'.asBuffer(), '2'.asBuffer(), '3'.asBuffer()],
                              columnParent,
                              multiGetSlicePredicate,
                              ConsistencyLevel.ONE)

    multiGetSliceMap.each { buffer, columnOrSuperColumns ->
        def start = buffer.position()
        def limit = buffer.limit()
        def bytes = new byte[limit - start]
        (start..<limit).eachWithIndex { c, i -> bytes[i] = buffer.get(c) }

        def key = new String(bytes, StandardCharsets.UTF_8)
        println("key => $key")

        columnOrSuperColumns.each { columnOrSuperColumn ->
            def column = columnOrSuperColumn.column
            println('get multi column: [name: value] => [' +
                        new String(column.name, StandardCharsets.UTF_8) + ': ' +
                        new String(column.value, StandardCharsets.UTF_8) + ']')
        }
    }

    // multiget_countを使って、複数ロウのカラム数を取得
    def multiGetCountPredicate = new SlicePredicate()
    multiGetCountPredicate.slice_range = new SliceRange(ByteBuffer.wrap(new byte[0]),
                                                        ByteBuffer.wrap(new byte[0]),
                                                        false,
                                                        100)

    def multiGetCountMap =
        client.multiget_count(['1'.asBuffer(), '2'.asBuffer(), '3'.asBuffer()],
                              columnParent,
                              multiGetCountPredicate,
                              ConsistencyLevel.ONE)

    multiGetCountMap.each { buffer, count ->
        def start = buffer.position()
        def limit = buffer.limit()
        def bytes = new byte[limit - start]
        (start..<limit).eachWithIndex { c, i -> bytes[i] = buffer.get(c) }

        def key = new String(bytes, StandardCharsets.UTF_8)
        println("key: count => $key: $count")
    }

    // get_range_slicesで、ロウキーの範囲で取得
    def rangeSlicesPredicate = new SlicePredicate()
    rangeSlicesPredicate.slice_range = new SliceRange(ByteBuffer.wrap(new byte[0]),
                                                      ByteBuffer.wrap(new byte[0]),
                                                      false,
                                                      100)

    def rangeSliceKeyRange = new KeyRange()
    // 全キー指定
    rangeSliceKeyRange.start_key = new byte[0]
    rangeSliceKeyRange.end_key = new byte[0]
    // とはいえ、RandomPartitionerではキーの範囲指定はできませんが…
    // InvalidRequestException(why:start key's token sorts after end key's token.  this is not allowed; you probably should not specify end key at all except with an ordered partitioner)
    //rangeSliceKeyRange.start_key = '1'.asBinary()
    //rangeSliceKeyRange.end_key = '2'.asBinary()

    def getRangeKeySlices =
        client.get_range_slices(columnParent,
                                rangeSlicesPredicate,
                                rangeSliceKeyRange,
                                ConsistencyLevel.ALL)

    getRangeKeySlices.each { keySlice ->
        def key = new String(keySlice.key, StandardCharsets.UTF_8)
        println("key => $key")

        keySlice.columns.each { columnOrSuperColumn ->
            def column = columnOrSuperColumn.column
            println('get range slices column: [name: value] => [' +
                        new String(column.name, StandardCharsets.UTF_8) + ': ' +
                        new String(column.value, StandardCharsets.UTF_8) + ']')
        }
    }

    // get_paged_sliceを使うと、get_range_slicesを少し簡単に呼べる
    def pagedKeyRange = new KeyRange()
    pagedKeyRange.start_key = new byte[0]
    pagedKeyRange.end_key = new byte[0]

    def getPagedKeySlices =
        client.get_paged_slice(columnFamilyName,
                               pagedKeyRange,
                               ByteBuffer.wrap(new byte[0]),  // start_columnを指定
                               ConsistencyLevel.ALL)
    // start_columnの指定は、最初の取得結果1件目にしか効果がない？？

    getPagedKeySlices.each { keySlice ->
        def key = new String(keySlice.key, StandardCharsets.UTF_8)
        println("key => $key")

        keySlice.columns.each { columnOrSuperColumn ->
            def column = columnOrSuperColumn.column
            println('get paged slice column: [name: value] => [' +
                        new String(column.name, StandardCharsets.UTF_8) + ': ' +
                        new String(column.value, StandardCharsets.UTF_8) + ']')
        }
    }

    // get_indexed_slicesを使った、セカンダリインデックスを使用する検索
    def indexPredicate = new SlicePredicate()
    indexPredicate.slice_range = new SliceRange(ByteBuffer.wrap(new byte[0]),  // start
                                                ByteBuffer.wrap(new byte[0]),  // finish
                                                false,  // reverse
                                                100)  // count

    def indexClause = new IndexClause([new IndexExpression('occupation'.asBuffer(),
                                                           IndexOperator.EQ,
                                                           'System Engineer'.asBuffer()),
                                          new IndexExpression('age'.asBuffer(),
                                                              IndexOperator.GTE,
                                                              '20'.asBuffer())],
                                      ByteBuffer.wrap(new byte[0]),  // start key
                                      100)  // count

    def getIndexedKeySlices =
        client.get_indexed_slices(columnParent,
                                  indexClause,
                                  indexPredicate,
                                  ConsistencyLevel.ALL)

    getIndexedKeySlices.each { keySlice ->
        def key = new String(keySlice.key, StandardCharsets.UTF_8)
        println("key => $key")

        keySlice.columns.each { columnOrSuperColumn ->
            def column = columnOrSuperColumn.column
            println('get indexed slice column: [name: value] => [' +
                        new String(column.name, StandardCharsets.UTF_8) + ': ' +
                        new String(column.value, StandardCharsets.UTF_8) + ']')
        }
    }
}

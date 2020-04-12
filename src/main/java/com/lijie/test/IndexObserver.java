package com.lijie.test;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class IndexObserver extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        CellScanner cellScanner = put.cellScanner();
        //handle rowkey
        String key = Bytes.toString(put.getRow());
        String[] split = key.split("-");
        String newKey = split[1] + "-" + split[0];

        boolean b = false;

        Put put1 = new Put(Bytes.toBytes(newKey));

        while (cellScanner.advance()) {
            Cell current = cellScanner.current();
            String columnName = new String(CellUtil.cloneQualifier(current), "utf-8");
            if (columnName.equals("FROM")){
                put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to"), CellUtil.cloneValue(current));
                b=true;
            }else if (columnName.equals("to")){
                put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from"), CellUtil.cloneValue(current));
                b=true;
            }
        }

        if (b){
            Connection connection = HbaseUtil1.getConnection();
            Table fans = connection.getTable(TableName.valueOf("fans"));
            fans.put(put1);
            HbaseUtil1.close();
        }

    }
}

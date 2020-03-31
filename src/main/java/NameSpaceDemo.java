import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class NameSpaceDemo {
    private static Admin admin = null;

    @Before
    public void init() {
        admin = HbaseUtil.getAdmin();
    }

    /**
     * 列举namespace
     */
    @Test
    public void liatNamaSpace() throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }
    }

    /**
     * 列举表文件
     *
     * @throws IOException
     */
    @Test
    public void listTable() throws IOException {
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor);
        }
    }

    /**
     * 列举某个namespace的表文件
     *
     * @throws IOException
     */
    @Test
    public void listNamaSpaceTables() throws IOException {
        HTableDescriptor[] lixiaojies = admin.listTableDescriptorsByNamespace("lixiaojie");
        for (HTableDescriptor lixiaojy : lixiaojies) {
            System.out.println(lixiaojy.getNameAsString());
        }
    }


    @Test
    public void modifyNamaSpace() throws IOException {

        NamespaceDescriptor qf = NamespaceDescriptor.create("lixiaojie").build();
        NamespaceDescriptor lixiaojie = admin.getNamespaceDescriptor("lixiaojie");
        admin.modifyNamespace(lixiaojie);
    }

    @Test
    public void describeNameSpace() throws IOException {
        NamespaceDescriptor lixoajie = admin.getNamespaceDescriptor("lixoajie");
        System.out.println(lixoajie.getName());

        Map<String, String> configuration = lixoajie.getConfiguration();
        for (Map.Entry<String, String> e : configuration.entrySet()) {
            System.out.println(e.getKey());
        }
    }


    @After
    public void close() {
        HbaseUtil.closeAdmin(admin);
    }
}

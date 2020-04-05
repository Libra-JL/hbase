import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class DruidPool {

    private static DataSource ds;
    private static ThreadLocal<Connection> local;

    static {
        try {
            Properties properties = new Properties();
            properties.load(DruidPool.class.getClassLoader().getResourceAsStream("druid.properties"));
            ds = DruidDataSourceFactory.createDataSource(properties);
            local = new ThreadLocal<>();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        Connection connection = local.get();
        if (connection == null) {
            connection = ds.getConnection();
            local.set(connection);
        }
        return connection;
    }

    public static void free() {

        try {
            Connection connection = local.get();
            if (connection != null) {
                local.remove();
                connection.setAutoCommit(true);
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public <E> ArrayList<E> getAll(Class<E> clazz,String sql,Object... args){

        return null;
    }


}

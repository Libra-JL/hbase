import java.sql.*;

public class TestDruidPool {
    public static void main(String[] args) throws SQLException {
        Connection connection = DruidPool.getConnection();
        String sql = "select * from websites";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            String id = resultSet.getString("url");
            String name = resultSet.getString("name");
            String url = resultSet.getString("url");
            String alexa = resultSet.getString("alexa");
            String country = resultSet.getString("country");
            System.out.println(id+"\t"+name+"\t"+url+"\t"+alexa+"\t"+country);
        }
        preparedStatement.close();
        DruidPool.free();

    }
}

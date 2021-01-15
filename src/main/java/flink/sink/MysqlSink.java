package flink.sink;


import flink.pojo.Broker;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: lsl
 * @Date: 2020/12/2 14:30
 * @Description:
 **/
public class MysqlSink extends RichSinkFunction<Broker> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into broker_count (province_id,province_name,region_id,region_name,city_id,city_name,count) values(?,?,?,?,?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(Broker value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setString(1, value.province_id);
        ps.setString(2, value.province_name);
        ps.setString(3, value.region_id);
        ps.setString(4, value.region_name);
        ps.setString(5, value.city_id);
        ps.setString(6, value.city_name);
        ps.setInt(7, value.count);
        ps.execute();

    }


    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }


}

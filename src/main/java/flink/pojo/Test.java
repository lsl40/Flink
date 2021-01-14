package flink.pojo;

import java.io.Serializable;

/**
 * @Author: lsl
 * @Date: 2021/1/13 15:03
 * @Description:
 **/
public class Test implements  Serializable{

    String id;
    int count;
    String time;


    public Test() {
    }

    public Test(String id, int count, String time) {
        this.id = id;
        this.count = count;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Test{" +
                "id='" + id + '\'' +
                ", count=" + count +
                ", time='" + time + '\'' +
                '}';
    }
}

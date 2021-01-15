package flink.pojo;

import java.io.Serializable;

/**
 * @Author: lsl
 * @Date: 2020/11/25 10:47
 * @Description:
 **/
public class CityProvinceRegion implements Serializable {
        public  String province_id;
        public  String province_name;
        public  String region_id;
        public  String region_name;
        public  String city_id;
        public  String city_name;
        public  String table_id;
        public  String table_dev_id;
        public  String dw_day;

        @Override
        public String toString() {
                return "CityProvinceRegion{" +
                        "province_id='" + province_id + '\'' +
                        ", province_name='" + province_name + '\'' +
                        ", region_id='" + region_id + '\'' +
                        ", region_name='" + region_name + '\'' +
                        ", city_id='" + city_id + '\'' +
                        ", city_name='" + city_name + '\'' +
                        ", table_id='" + table_id + '\'' +
                        ", table_dev_id='" + table_dev_id + '\'' +
                        ", dw_day='" + dw_day + '\'' +
                        '}';
        }

        public String getProvince_id() {
                return province_id;
        }

        public void setProvince_id(String province_id) {
                this.province_id = province_id;
        }

        public String getProvince_name() {
                return province_name;
        }

        public void setProvince_name(String province_name) {
                this.province_name = province_name;
        }

        public String getRegion_id() {
                return region_id;
        }

        public void setRegion_id(String region_id) {
                this.region_id = region_id;
        }

        public String getRegion_name() {
                return region_name;
        }

        public void setRegion_name(String region_name) {
                this.region_name = region_name;
        }

        public String getCity_id() {
                return city_id;
        }

        public void setCity_id(String city_id) {
                this.city_id = city_id;
        }

        public String getCity_name() {
                return city_name;
        }

        public void setCity_name(String city_name) {
                this.city_name = city_name;
        }

        public String getTable_id() {
                return table_id;
        }

        public void setTable_id(String table_id) {
                this.table_id = table_id;
        }

        public String getTable_dev_id() {
                return table_dev_id;
        }

        public void setTable_dev_id(String table_dev_id) {
                this.table_dev_id = table_dev_id;
        }

        public String getDw_day() {
                return dw_day;
        }

        public void setDw_day(String dw_day) {
                this.dw_day = dw_day;
        }
}

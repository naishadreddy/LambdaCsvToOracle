package com.ingest;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Date;

@JsonPropertyOrder({ "unique_user_id","user_pool_name","custom_activity","custom_device_code","custom_year","gender","is_smoker","name","phone_number","user_create_date","user_email","user_last_modified_date","user_name","user_pool_id","user_status"
})
public class User {

    public String unique_user_id;
    public String user_pool_name;
    public int custom_activity;
    public String custom_device_code;
    public int custom_year;
    public String gender;
    public String is_smoker;
    public String name;
    public String phone_number;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss.SSSSSSXXX")
    public Date user_create_date;
    public String user_email;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss.SSSSSSXXX")
    public Date user_last_modified_date;
    public String user_name;
    public String user_pool_id;
    public String user_status;


    @Override
    public String toString() {
        return "User{" +
                "unique_user_id='" + unique_user_id + '\'' +
                ", user_pool_name='" + user_pool_name + '\'' +
                ", custom_activity=" + custom_activity +
                ", custom_device_code='" + custom_device_code + '\'' +
                ", custom_year=" + custom_year +
                ", gender='" + gender + '\'' +
                ", is_smoker='" + is_smoker + '\'' +
                ", name='" + name + '\'' +
                ", phone_number='" + phone_number + '\'' +
                ", user_create_date=" + user_create_date +
                ", user_email='" + user_email + '\'' +
                ", user_last_modified_date='" + user_last_modified_date + '\'' +
                ", user_name='" + user_name + '\'' +
                ", user_pool_id='" + user_pool_id + '\'' +
                ", user_status='" + user_status + '\'' +
                '}';
    }
}

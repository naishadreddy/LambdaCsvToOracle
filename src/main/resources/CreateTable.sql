
CREATE TABLE "UserData"
(
  "unique_user_id" VARCHAR2(50) PRIMARY KEY ,
  "user_pool_name" VARCHAR2(50),
   "custom_activity" INT,
    "custom_device_code" VARCHAR2(200),
    "custom_year" INT,
    "gender" VARCHAR2(20),
     "is_smoker" VARCHAR2(20),
      "name" VARCHAR2(200),
      "phone_number" VARCHAR2(200),
       "user_create_date" TIMESTAMP,
        "user_email" VARCHAR2(200),
         "user_last_modified_date" TIMESTAMP,
         "user_name" VARCHAR2(200),
         "user_pool_id" VARCHAR2(200),
         "user_status" VARCHAR2(200)
);
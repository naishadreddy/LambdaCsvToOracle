package com.ingest;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

// com.ingest.Handler value: example.com.ingest.Handler
public class Handler implements RequestHandler<Map<String,String>, String>{


    static  LambdaLogger logger ;
    //private static final System.Logger logger = LoggerFactory.getLogger(com.ingest.Handler.class);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    @Override
    public String handleRequest(Map<String,String> event, Context context)
    {
        logger = context.getLogger();
        String response = new String("200 OK");
        // log execution details

        final List<String> lambdaConfig = List.of("s3Region","s3Bucket","s3ObjectKey","fileName","fileDelimiter");
        Map<String,String> envConfigMap = System.getenv().entrySet().stream().filter(e -> lambdaConfig.contains(e.getKey()))
                .collect(Collectors.toMap(e->e.getKey(),e->e.getValue()));

        //if env configs are missing exit execution
        if(envConfigMap.keySet().stream().count() != lambdaConfig.stream().count() )
            return new String(" please check env config. Missing some properties ");

        logger.log("ENVIRONMENT VARIABLES from lambda config " + gson.toJson(envConfigMap));

        logger.log("CONTEXT: " + gson.toJson(context));
        // process event
        logger.log("EVENT: " + gson.toJson(event));
        logger.log("EVENT TYPE: " + event.getClass().toString());

        String s3Region = System.getenv("s3Region");String s3Bucket = System.getenv("s3Bucket");String s3ObjectKey = System.getenv("s3ObjectKey");
        String fileName = System.getenv("fileName");char delimiter = System.getenv("fileDelimiter").charAt(0);

        try {
           List<User> users =  getUserListFromS3CsvFile(s3Region,s3Bucket,s3ObjectKey,fileName,delimiter);

           //CONNECT TO ORACLE DB
            insertData2OracleDB(users, System.getenv());

           // logger.log("csv input stream " + csvInputStream);
           // logger.log("POJO Mapped data is " + csvObjectMapper(csvInputStream,true,delimiter,User.class));
        } catch (IOException | SQLException e) {
            logger.log(e.getLocalizedMessage());
            return new String(" Reading CSV File Failed.\n 500 Bad \n Internal Server Error");
        }

        return response;
    }




    public static List<User> getUserListFromS3CsvFile(String region, String bucketName, String key, String fileName,char delimiter) throws IOException {

       Regions clientRegion = Regions.valueOf(region);
       ResponseHeaderOverrides headerOverrides;
       S3Object headerOverrideObject = null;
       List<User> userList = null;
       logger.log("reading csvfile " + key +" from s3 bucket .... "+ bucketName );
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();

            // Get an entire object, overriding the specified response headers, and print the object's content.
             headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename="+fileName);

            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);

            logger.log(" S3 object get request --> " + getObjectRequestHeaderOverride);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            logger.log(" header override object  --> " + headerOverrideObject);

            userList = csvObjectMapper(headerOverrideObject.getObjectContent(),true,delimiter,User.class);
            logger.log("POJO Mapped data is " + userList);

//           logger.log("POJO mapped" + csvObjectMapper(headerOverrideObject.getObjectContent(),true, ',',User.class));


        } catch (SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }// Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
        finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
//        assert headerOverrideObject != null;
//        return headerOverrideObject.getObjectContent();
        return userList;
    }



    private static <T> List<T> csvObjectMapper(InputStream input, boolean withHeaders, char separator, Class<T> mapperClass) throws IOException {
        CsvMapper mapper = new CsvMapper();

        mapper.enable(CsvParser.Feature.TRIM_SPACES);
        mapper.enable(CsvParser.Feature.ALLOW_TRAILING_COMMA);
        mapper.enable(CsvParser.Feature.INSERT_NULLS_FOR_MISSING_COLUMNS);
        mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
       // mapper.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
        CsvSchema schema = mapper.schemaFor(mapperClass).withColumnReordering(true);
        ObjectReader reader;
        if (separator == '\t') {
            schema = schema.withColumnSeparator('\t');
        }
        else {
            schema = schema.withColumnSeparator(',');
        }

        if (withHeaders) {
            schema = schema.withHeader();
        }
        else {
            schema = schema.withoutHeader();
        }
        reader = mapper.readerFor(mapperClass).with(schema);
        return reader.<T>readValues(input).readAll();
    }


    public static <T> String insertData2OracleDB(List<T> users, Map<String,String> envConfig) throws SQLException {

         DatabaseUtil dbUtil = new DatabaseUtil();
         DatabaseCredentials dbCreds = new DatabaseCredentials();
         dbCreds.setUserName(Optional.ofNullable(envConfig.get("dbUser")).orElse("admin"));
         dbCreds.setDbHost(Optional.ofNullable(envConfig.get("dbHost")).orElse("test.cn7bgbs2xvwq.us-east-2.rds.amazonaws.com"));
         dbCreds.setPassword(Optional.ofNullable(envConfig.get("dbPassword")).orElse("admin123"));
         dbCreds.setDbPort(Optional.ofNullable(envConfig.get("dbPort")).orElse("1521"));
        dbCreds.setDbName(Optional.ofNullable(envConfig.get("dbName")).orElse("test"));

        Connection connection = dbUtil.getConnection(dbCreds);
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO admin.\"UserData\"(\"unique_user_id\",\"user_pool_name\"," +
                "\"custom_activity\",\"custom_device_code\",\"custom_year\",\"gender\",\"is_smoker\",\"name\",\"phone_number\",\"user_create_date\"," +
                "\"user_email\",\"user_last_modified_date\",\"user_name\",\"user_pool_id\",\"user_status\"" +")"
                +" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {

            users.stream().forEach(o -> {
                try {
                    insert((User)o,stmt);
                } catch (SQLException throwables) {
                    logger.log(" inside insert block ");
                }
            });

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        finally {
            connection.close();
        }

        return "200 records inserted ";

    }

    private static void insert(User object, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, object.unique_user_id);
        stmt.setString(2, object.user_pool_name);
        stmt.setInt(3, object.custom_activity);
        stmt.setString(4, object.custom_device_code);
        stmt.setInt(5, object.custom_year);
        stmt.setString(6, object.gender);
        stmt.setString(7, object.is_smoker);
        stmt.setString(8, object.name);
        stmt.setString(9, object.phone_number);
        stmt.setDate(10, new java.sql.Date(object.user_create_date.getTime()));
        stmt.setString(11, object.user_email);
        stmt.setDate(12,new java.sql.Date(object.user_last_modified_date.getTime()));
        stmt.setString(13, object.user_name);
        stmt.setString(14, object.user_pool_id);
        stmt.setString(15, object.user_status);

        logger.log(stmt.toString());
        stmt.executeUpdate();
    }



}

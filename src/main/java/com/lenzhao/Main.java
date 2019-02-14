package com.lenzhao;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.util.Iterator;

/**
 * @author lenzhao
 * @date 2019/2/14 9:39
 * @description TODO
 */
public class Main {

    public static void main(String[] args) throws Exception {
        serialize();
        deserialize();
    }

    public static void serialize() throws Exception {

        Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("user.avsc"));
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Format");
        user1.put("favorite_number", 666);
        user1.put("favorite_color", "red");

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Format2");
        user2.put("favorite_number", 66);
        //user2.put("favorite_color", null);

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, new File("d:/users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    public static void deserialize() throws Exception {
        Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("user.avsc"));
        File file = new File("d:/users.avro");
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while(dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
            formatJsonForUnionTypeInAvro(user.toString());
        }
    }

    public static void formatJsonForUnionTypeInAvro(String json) throws JSONException {
        JSONObject jsonObject = JSONObject.fromObject(json);
        Iterator iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = (String)iterator.next();
            String value = jsonObject.get(key).toString();
            String type = jsonObject.get(key).getClass().getCanonicalName();
            System.out.println("key=" + key + " and value = " + value + ", type= " + type);
        }
    }
}

package org.example;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVReader {

    public static void readdata(){
        String line = "";
        String splitBy = ",";
        try
        {
//parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Praneeth\\DE\\DE_week1\\src\\main\\java\\org\\example\\employees.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] employee = line.split(splitBy);    // use comma as separator
                HashMap<String, String> value = new HashMap<String, String>();
                value.put("EMPLOYEE_ID", employee[0]);
                value.put("FIRST_NAME", employee[1]);
                value.put("LAST_NAME", employee[2]);
                value.put("EMAIL", employee[3]);
                value.put("PHONE_NUMBER", employee[4]);
                System.out.println(value);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    public static void main(String[] args){
        readdata();
    }

}

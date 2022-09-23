package com.sync.test.sync_test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class ConvertSQL {
    private String url = "jdbc:postgresql://localhost:5433/studentdb2";
    private String user = "user2";
    private String password = "password2";
    
    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);        
    }
    
    public void insert(JSONObject convertingData ,Map<String, String> type_field_Map ,String table){
        System.out.println("----組裝SQL-----");
        String SQL1 = "INSERT INTO "+ table +" (";
        Integer counter = 0;
        Integer counter2 = 0;       
        String SQL2 = " VALUES(";

        //組裝SQL
        for (Map.Entry<String, String> entry : type_field_Map.entrySet()) {
            counter++;
            SQL1 +=entry.getKey();
            SQL2+="?";
            if (counter < type_field_Map.size()){
                SQL1+=", ";
                SQL2+=",";
            }        
        }
        String SQL = SQL1 +")" + SQL2+")";
        System.out.println(SQL);

        try (Connection conn = connect();
                PreparedStatement pstmt = conn.prepareStatement(SQL)){
                    for (Map.Entry<String, String> entry : type_field_Map.entrySet()) {
                        counter2++;
                        // System.out.println(counter2);                                  
                        switch(entry.getValue()){
                            case "int32":
                                pstmt.setInt(counter2,(Integer)convertingData.get(entry.getKey()));
                                break;
                            case "string":
                                pstmt.setString(counter2, convertingData.get(entry.getKey()).toString());
                                break;
                            default:
                                System.out.println("未設置的type");
                        }
                        System.out.println(pstmt);                 
                    }               
                    
                    pstmt.executeUpdate();
                    System.out.println("----insert Data後-----");
                    System.out.println(pstmt);                    

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public void update(JSONObject convertingData ,Map<String, String> type_field_Map ,String table,JSONArray field_key_name){
        System.out.println("----update Data前-----");
        System.out.println(convertingData);
        System.out.println(type_field_Map);
        System.out.println(field_key_name.toString());

        System.out.println("----組裝SQL-----");
        String SQL1 = "UPDATE "+ table +" SET ";
        Integer counter = 0;
        Integer counter2 = 0;
        Integer key_counter = 0;   
        String SQL2 = " WHERE ";

        //JSONArray 轉為 List
        ArrayList<String> key_list = new ArrayList<>();
        for(int i=0;i<field_key_name.length();i++){
            key_list.add(field_key_name.get(i).toString());                           
        }
        System.out.println(key_list);  
	    

        //組裝SQL
        for (Map.Entry<String, String> entry : type_field_Map.entrySet()) {
            counter++;
            SQL1 +=entry.getKey()+"=?";
            if (counter < type_field_Map.size()){                
                SQL1+=", "; 
            }                        
        }

        for(int i=0;i<field_key_name.length();i++){
            SQL2 +=field_key_name.get(i)+"=?";
            if (counter < field_key_name.length()){
                SQL2+="AND ";                
            }                 
        }       
            
        String SQL = SQL1 + SQL2;
        System.out.println(SQL);
        
        //組裝SQL值
        try (Connection conn = connect();
        PreparedStatement pstmt = conn.prepareStatement(SQL)){
            for (Map.Entry<String, String> entry : type_field_Map.entrySet()) {
                counter2++;
                System.out.println(counter2);
                                   
                switch(entry.getValue()){
                    case "int32":
                        pstmt.setInt(counter2,(Integer)convertingData.get(entry.getKey()));

                        //如果欄位有在field_key中,代表為Primary or Unique key,將此欄位放在WHERE
                        if(key_list.contains(entry.getKey())){
                            key_counter++;
                            pstmt.setInt(type_field_Map.size()+ key_counter,(Integer)convertingData.get(entry.getKey()));
                        }
                        break;
                    case "string":
                        pstmt.setString(counter2, convertingData.get(entry.getKey()).toString());
                        
                        //如果欄位有在field_key中,代表為Primary or Unique key,將此欄位放在WHERE
                        if(key_list.contains(entry.getKey())){
                            key_counter++;
                            pstmt.setString(type_field_Map.size()+ key_counter, convertingData.get(entry.getKey()).toString());
                        }
                        break;
                    default:
                        System.out.println("未設置的type");
                }                              
                
                System.out.println(pstmt);                 
            }       
            
            pstmt.executeUpdate();
            System.out.println("----update Data後-----");
            System.out.println(pstmt);                    

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        
    }

    public void delete(JSONObject convertingData ,Map<String, String> type_field_Map ,String table,JSONArray field_key_name){
        System.out.println("----delete Data前-----"); 
        System.out.println(convertingData);
        System.out.println(type_field_Map);
        System.out.println(field_key_name.toString());
                  
        System.out.println("----組裝SQL-----");
        String SQL1 = "DELETE FROM "+ table;
        String SQL2 = " WHERE ";      
        Integer counter = 0;
        Integer counter2 = 0;       

        //組裝SQL
        for (int i=0;i<field_key_name.length();i++) {
            counter++;
            SQL2 +=field_key_name.get(i)+"=?";
                if (counter < field_key_name.length()){
                    SQL2+=" and ";                
                }  
                
        }
        String SQL = SQL1 + SQL2;
        System.out.println(SQL);

        //組裝SQL值
        try (Connection conn = connect();
                PreparedStatement pstmt = conn.prepareStatement(SQL)){

                    for(int i=0;i<field_key_name.length();i++){
                        counter2++;
                        String key = field_key_name.get(i).toString();
                        switch(type_field_Map.get(key)){
                            case "int32":
                                pstmt.setInt(counter2,(Integer)convertingData.get(key));
                                break;
                            case "string":
                                pstmt.setString(counter2, convertingData.get(key).toString());
                                break;
                            default:
                                System.out.println("未設置的type");

                        }
                        
                    }

                    // for (Map.Entry<String, String> entry : type_field_Map.entrySet()) {
                    //     counter2++;
                    //     System.out.println(counter2);                                  
                    //     switch(entry.getValue()){
                    //         case "int32":

                    //             pstmt.setInt(counter2,(Integer)convertingData.get(entry.getKey()));
                    //             break;
                    //         case "string":
                    //             pstmt.setString(counter2, convertingData.get(entry.getKey()).toString());
                    //             break;
                    //         default:
                    //             System.out.println("未設置的type");
                    //     }
                    //     System.out.println(pstmt);                 
                    // }               
                    
                    pstmt.executeUpdate();
                    System.out.println("----delete Data後-----");
                    System.out.println(pstmt);                    

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        // System.out.println(SQL);
        // String SQL = "DELETE FROM "+ table + " WHERE ID = ?";
        // try (Connection conn = connect();
        //         PreparedStatement pstmt = conn.prepareStatement(SQL)){
        //     pstmt.setInt(1, deleteId);           
            
        //     pstmt.executeUpdate();            
        //     System.out.println(pstmt);
        //     System.out.println("----delete Data後-----");  

        // } catch (SQLException ex) {
        //     System.out.println(ex.getMessage());
        // }

    }

}

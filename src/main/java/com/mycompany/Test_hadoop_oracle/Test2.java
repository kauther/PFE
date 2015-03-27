package com.mycompany.Test_hadoop_oracle;

import java.sql.*;

public class Test2 {
 
    Connection maConnexion;
     
    public void ouvrirConnexion(String url) {
        try{

            Class.forName("oracle.jdbc.driver.OracleDriver");

            maConnexion=DriverManager.getConnection(url);
             
            System.out.println("Connexion établie !");
        }
         
        catch(ClassNotFoundException e){
            System.out.println("Impossible de charger le pilote!");
            return;
        }
         
        catch(SQLException e){
            System.out.println("Connexion impossible !");
            return;
        }
         
        }
 
    public static void main(String[] args) throws SQLException{
        String url="jdbc:oracle:thin:localhost:1521:xe";
        Test2 bd = new Test2 ();
        bd.ouvrirConnexion(url);
     
}
     
}
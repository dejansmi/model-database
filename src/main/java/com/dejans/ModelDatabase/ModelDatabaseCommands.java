package com.dejans.ModelDatabase;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.dejans.model.TreeMapYamlParse;
import com.dejans.model.ModelDefinitionTree;

public class ModelDatabaseCommands {
    private String microserviceName;
    private String databasetype;
    private String connectString;
    private String baseDir;
    ModelDefinitionTree modelTree;

    class ColumnObject {
        String columnName = new String();
        String itemName = new String();
        // status:
        //    0 - not checked 
        //    1 - checked but not exist at other side (database or model)
        //    2 - checked and exist but no same all details 
        //    9 - checked and the same
        int status = 0;

    }

    class TableObject {
        String table;
        // status:
        //    0 - not checked 
        //    1 - checked but not exist at other side (database or model)
        //    2 - checked and exist but no same all details 
        //    9 - checked and the same
        int status = 0;
        Map<String, ColumnObject> columns = new HashMap<String, ColumnObject>();

    }

    public ModelDatabaseCommands(String[] args) {
        boolean nextdatabasetype = false;
        boolean nextconnect = false;

        this.baseDir = System.getenv("JAVA_PROJECTS_BASE");

        if (args.length <= 0) {
            printSyntax();
            System.exit(1);
        }
        for (int i = 0; i < args.length; i++) {
            if (i == 0) {
                if (args[i].startsWith("--")) {
                    System.out.println("Error: First argument have to be microservice name");
                    System.exit(1);
                }
                microserviceName = args[i];
            }
            if (nextdatabasetype) {
                nextdatabasetype = false;
                if (args[i].startsWith("--")) {
                    System.out.println("Error: after --databasetype have to come database type");
                    System.exit(1);
                }
                databasetype = args[i];
            }
            if (args[i].startsWith("--databasetype")) {
                nextdatabasetype = true;
            }

            if (nextconnect) {
                nextconnect = false;
                if (args[i].startsWith("--")) {
                    System.out.println("Error: after --connect have to come connect string");
                    System.exit(1);
                }
                connectString = args[i];
            }
            if (args[i].startsWith("--connect")) {
                nextconnect = true;
            }
        } //for

        // impput arguments have to defined
        if (connectString == null || connectString == "") {
            System.out.println("Error: connect string have to be defined");
            System.exit(1);
        }
        if (databasetype == null || databasetype == "") {
            System.out.println("Error: database type have to be defined");
            System.exit(1);
        }

        Connection con = connect(databasetype, connectString);
        try {
            String baseDirFiles = this.baseDir + "/source/" + microserviceName + "/Files/";
            File fileDomains = new File(baseDirFiles + "domains.yml");
            File fileModel = new File(baseDirFiles + "model.yml");
            File fileDatabases = new File(baseDirFiles + "databases.yml");
            File fileApi = new File(baseDirFiles + "api.yml");
            TreeMapYamlParse tMYP = new TreeMapYamlParse(fileDomains, fileModel, fileDatabases, fileApi);
            modelTree = new ModelDefinitionTree(tMYP);
            System.out.println(tMYP.toString());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Map<String, TableObject> tables = new HashMap<String, TableObject>();
        String obj = new String();
        while (obj != null) {
            obj = modelTree.nextElement(obj, "Models");
            if (obj != null) {
                System.out.println(obj);
                if (modelTree.getModelsType(obj).equals("array") || modelTree.getModelsType(obj).equals("list")) {
                    if (modelTree.getDatabaseType(obj).equals("table")) {
                        String table = modelTree.getDatabaseTable(obj);
                        TableObject tabObj = new TableObject();
                        tabObj.table = table;
                        String item = new String();
                        while (item != null) {
                            item = modelTree.nextItem(obj, item);
                            if (item != null) {
                                String columnName = modelTree.getColumn(obj, item);
                                System.out.println(item + ":" + columnName);
                                if (columnName != null && !columnName.equals("")) {
                                    ColumnObject colObj = new ColumnObject();
                                    colObj.columnName = columnName;
                                    colObj.itemName = item;
                                    tabObj.columns.put(columnName.toUpperCase(), colObj);
                                }
                            }
                        }
                        tables.put(table.toUpperCase(), tabObj);
                        System.out.println("TABLE:" + table);
                    }
                }
            }
        }

        try {
            System.out.println("DATABASE TABLES");
            PreparedStatement sqlSts = null;
            String sqlTablesAllInScheme = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            sqlSts = con.prepareStatement(sqlTablesAllInScheme);

            ResultSet rs = sqlSts.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                System.out.println(tableName);
                String sqlColumnsInTable = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name   = ?";
                sqlSts = con.prepareStatement(sqlColumnsInTable);
                sqlSts.setString(1, tableName);
                ResultSet cs = sqlSts.executeQuery();

                while (cs.next()) {
                    String columnName = cs.getString("column_name");
                    System.out.println(columnName);
                }
                

            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        for (String key : tables.keySet()) {
            System.out.println("key : " + key + "  ");
            TableObject tabObj = tables.get(key);
            System.out.println("table : " + tabObj.table + "  ");
            for (String keyCol : tabObj.columns.keySet()) {
                System.out.println("keyCol : " + keyCol + "  ");
                ColumnObject colObj = tabObj.columns.get(keyCol);
                System.out.println("Column:" + colObj.columnName);
                System.out.println("Item:" + colObj.itemName);
            }

        }

    }

    private void printSyntax() {
        System.out.println(
                "Syntax is: model-database microserviceName --databasetype DatabaseType --connect ConnectString");
        System.out.println("Purpose: synchronize model in the microservice microserviceName and database");
        System.out.println("Options are:");
        System.out.println("microserviceName - name of microservice or project name  ");
        System.out.println("--databasetype DatabaseType - database type for synchronization (postgres, oracle, mssql)");
        System.out.println(
                "--connect ConnectString - connect string for connection to database. Depand of database type");
        System.out.println(
                "  postgres connect string format: jdbc:postgresql://localhost:5432/postgres@username/password");
    }

    private Connection connect(String databasetype, String connectString) {
        if (databasetype.equals("postgres")) {
            String url = new String();
            String userName = new String();
            String password = new String();
            int i = connectString.indexOf('@');
            if (i > -1) {
                url = connectString.substring(0, i);
                int j = connectString.indexOf('/', i);
                userName = connectString.substring(i + 1, j);
                password = connectString.substring(j + 1);

                Connection con;
                try {
                    System.out.println("Connecting...");
                    con = DriverManager.getConnection(url, userName, password);
                    con.setAutoCommit(false);
                    System.out.println("Connected");
                    return con;
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return null;
            } else {
                System.out.println(
                        "Error: connect string for postgres have to be in format: jdbc:postgresql://localhost:5432/postgres@username/password");
                System.exit(1);
            }
            return null;
        }
        return null;
    }
}
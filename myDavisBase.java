package db;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.Stack;
import java.util.HashMap;
import java.lang.reflect.Field;
import java.lang.Object;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import static java.lang.System.err;
import static java.lang.System.out;

public class myDavisBase {
    static String copyright = "Copyright @ Subhajit Chatterjee";
    static boolean exitProgram = false;
    static String prompt = "sql> ";
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");
    static String version = "v1.0";

    /* This static variable controls page size. */
    static int pageSizePower = 9;
    /* This strategy insures that the page size is always a power of 2. */
    static int pageSize = (int)Math.pow(2, pageSizePower);
    static String err_msg="Unexpected syntax. Please use 'help' command to check supported commands";
    static String[] datatypes = {"null", "tinyint", "byte", "smallint","short",
            "int", "integer", "bigint","long", "float", "double",
            "real","year","time","datetime","date","text"};
    static String[] storedDatatypes = {"null", "byte", "byte", "short","short",
            "int", "int", "long","long", "float", "double",
            "double","byte","int","long","long","line"};

    static int [] decimalCodes = {0,1,1,2,2,3,3,4,4,5,6,6,8,9,10,11,12};
    static int [] contentSizes={0,1,1,2,2,4,4,8,8,4,8,8,1,4,8,8,0};
    static HashMap<String, Integer> dataTypeCodes= new HashMap<String, Integer>();
    static HashMap<Integer, String> codeDataTypes=new HashMap<Integer, String>();
    static HashMap<String, Integer> dataTypeSizes=new HashMap<String, Integer>();
    static HashMap<String, String> dataTypeMapping=new HashMap<String, String>();
    static void Map() {
        for (int i = 0; i < datatypes.length; i++) {
            dataTypeCodes.put(datatypes[i], decimalCodes[i]);
            codeDataTypes.put(decimalCodes[i], datatypes[i]);
            dataTypeSizes.put(datatypes[i], contentSizes[i]);
            dataTypeMapping.put(datatypes[i], storedDatatypes[i]);
        }
    };


    static int getDataTypeCode(String s)
    {
        return dataTypeCodes.getOrDefault(s, 12);
    }
    static String getCodeDataType(int code)
    {
        return codeDataTypes.getOrDefault(code, "text");
    }
    static int getDataTypeSize(String s)
    {
        return dataTypeSizes.getOrDefault(s, 0);
    }
    static String getStoredDataType(String s)
    {
        return dataTypeMapping.getOrDefault(s, "line");
    }


    public static byte[] hexToByteArr(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public static int createPage(RandomAccessFile pageFile,boolean leafPage,int parentPage) {
        try {
            pageFile.setLength(pageFile.length() + pageSize);
            long seekIndex=pageFile.length() - pageSize;
            pageFile.seek(seekIndex);

//            *** Page Header Section ***
            // first 1 byte : leaf page, next 1 byte: unused, next 2 bytes : 0 cells in page
            if (leafPage) {
                pageFile.write(hexToByteArr("0D000000"));
            } else {
                pageFile.write(hexToByteArr("05000000"));
            }
            // next two bytes : no offset yet set to end of new file length
            pageFile.writeShort((int) pageFile.length());
            //from position 6 to 9 : right sibling/child page pointer ,0xffffffff by default
            pageFile.seek(seekIndex+6);
            pageFile.write(hexToByteArr("ffffffff"));
//            position 10 to 13 : parent page pointer, 0xffffffff by default : root
            if (parentPage != -1)
            {
                pageFile.writeInt(parentPage);
            }
            else {
                pageFile.seek(seekIndex+10);
                pageFile.write(hexToByteArr("ffffffff"));
            }
            pageFile.seek(seekIndex+14);
//            position 14 and 15 : unused two bytes
            pageFile.write(hexToByteArr("0000"));
//            *** Page Header Section Completed ***
            return (int) ((pageFile.length() / pageSize) - 1);
        } catch (Exception e) {
            System.out.println(err_msg);
        }
        return 0;
    }




    public static void insertIntoMetaTable(String tableName,short root_page,int last_id, int record_count,byte isActive) {

        try {
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            int rowid = getfromMetaTable("davisbase_tables", false, true) + 1;
//            out.println("row id meta table :" +rowid);
            tableName = tableName + "\n";
//            out.println("table name: "+tableName);
            davisbaseTablesCatalog.seek(4);
            int lastWriteCell = (davisbaseTablesCatalog.readShort());
//            out.println("last written cell:"+lastWriteCell);

            int tableNameLen = tableName.length();
//            out.println("table name length: "+tableNameLen);
//            out.println("text data type size:" +getDataTypeSize("text"));

            int payload_size = 1 + 5 + (getDataTypeSize("text")+tableNameLen) +getDataTypeSize("smallint")  + getDataTypeSize("int")  + getDataTypeSize("int") +getDataTypeSize("byte") ;
//            out.println("payload size: "+payload_size);
            int headerSize = 6;
            int record_size = headerSize + payload_size;
            int cellToWrite = lastWriteCell - record_size;
//            out.println(" new cellToWrite: "+cellToWrite);


            int pageHeaderPointer = 16 + (2 * (rowid));
//            out.println(" new cellToWrite: "+cellToWrite);
//            out.println(" pageHeaderPointer: "+pageHeaderPointer);

            if (pageHeaderPointer < cellToWrite) {
                davisbaseTablesCatalog.seek(0);
//                out.println("page starting byte"+davisbaseTablesCatalog.readByte());
                davisbaseTablesCatalog.seek(2);
                davisbaseTablesCatalog.writeShort(rowid);

                davisbaseTablesCatalog.seek(4);
                davisbaseTablesCatalog.writeShort(cellToWrite);

                davisbaseTablesCatalog.seek(pageHeaderPointer-2);
                davisbaseTablesCatalog.writeShort(cellToWrite);

                davisbaseTablesCatalog.seek(cellToWrite);
                davisbaseTablesCatalog.writeShort(payload_size);
                davisbaseTablesCatalog.writeInt(rowid);
                davisbaseTablesCatalog.writeByte(5); // five cols in database_table
                davisbaseTablesCatalog.writeByte(getDataTypeCode("text")+tableNameLen); //tableName
                davisbaseTablesCatalog.writeByte(getDataTypeCode("smallint"));
                davisbaseTablesCatalog.writeByte(getDataTypeCode("int"));
                davisbaseTablesCatalog.writeByte(getDataTypeCode("int"));
                davisbaseTablesCatalog.writeByte(getDataTypeCode("byte"));
                davisbaseTablesCatalog.writeBytes(tableName);
                davisbaseTablesCatalog.writeShort(root_page);
                davisbaseTablesCatalog.writeInt(last_id);
                davisbaseTablesCatalog.writeInt(record_count);
                davisbaseTablesCatalog.writeByte(isActive);

                updateMetaTable(davisbaseTablesCatalog, "davisbase_tables", rowid, false, true,false);

            }

            davisbaseTablesCatalog.close();

        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }


    public static void updateMetaTable(RandomAccessFile davisbaseTablesCatalog, String tableName, int value, boolean updateRootPage, boolean updateRecordCount,boolean dropTable)
    {
//        out.println("entered update meta table");
//        out.println("value: "+value);
        try {
            int start = 0;
            int rightSiblingPointer = 0;
            int page = 0;
            int numRecords = 0;
            while (rightSiblingPointer != -1) {
                start = (int) (page * pageSize);
                davisbaseTablesCatalog.seek(start + 2);
//                out.println("updt method value"+value);
                numRecords = davisbaseTablesCatalog.readShort();
//                out.println("updt method numRecords"+numRecords);

                for (int i = 0; i < numRecords; i++) {
                    int posToRead = start + 16 + (2 * i); // cell position
                    davisbaseTablesCatalog.seek(posToRead);
                    posToRead = davisbaseTablesCatalog.readShort();
//                    out.println("posToRead: "+ posToRead);

                    davisbaseTablesCatalog.seek(posToRead);
                    int payloadSize = davisbaseTablesCatalog.readShort();
//                    out.println("payloadSize updtmethod: "+ payloadSize);
                    davisbaseTablesCatalog.seek(posToRead + 6); // cell header = 6 bytes
                    int numCols = davisbaseTablesCatalog.readByte();
//                    out.println("numCols updtmethod: "+ numCols);

                    davisbaseTablesCatalog.seek(posToRead + 6 + 1 + numCols); // start of record body

                    String tableNamePresent = davisbaseTablesCatalog.readLine();
//                    out.println("tableNamePresent updtmethod: "+ tableNamePresent);
                    if (tableName.equals(tableNamePresent)) {
//                        out.println("enter match");
                        if(dropTable)
                        {
//                            out.println("entered update active indicator");
//                            out.println("position to active indicator"+((posToRead + 6 + payloadSize)- 1));
                            davisbaseTablesCatalog.seek((posToRead + 6 + payloadSize) - 1);
                            davisbaseTablesCatalog.write(value);
                        }
                        if (updateRootPage) {
//                            out.println("entered update root page");
//                            out.println("position to write root page"+((posToRead + 6 + payloadSize)- 11));
                            davisbaseTablesCatalog.seek((posToRead + 6 + payloadSize) - 11);
                            davisbaseTablesCatalog.writeShort(value);
                        }
                        if (updateRecordCount) {
//                            out.println("entered update record count");
//                            out.println("position to write rec count"+((posToRead + 6 + payloadSize)- 5));
                            davisbaseTablesCatalog.seek(((posToRead + 6 + payloadSize)- 5));
                            davisbaseTablesCatalog.writeInt(value);
                        }

                    }
                }
                davisbaseTablesCatalog.seek(start + 6);
                rightSiblingPointer = davisbaseTablesCatalog.readInt();
                page = rightSiblingPointer;
            }

        } catch (Exception e) {
            System.out.println(err_msg);
        }
    }



    public static int getfromMetaTable(String tableName, boolean getRootPage, boolean getRecordCount) {
        int rootPageOrRecCount = 0;
        if (getRootPage) {
            rootPageOrRecCount = -1;
        }
        if (getRecordCount) {
            rootPageOrRecCount = 0;
        }
        try {
//            out.println("reached meta table");
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "r");
            int start = 0;
            int rightSiblingPointer = 0;
            int page = 0;
            int numRecords = 0;
            while (rightSiblingPointer != -1) // 0xffffffff = -1
            {
                start = (int) (page * pageSize);

                davisbaseTablesCatalog.seek(start + 2);
                numRecords = davisbaseTablesCatalog.readShort();

                for (int i = 0; i < numRecords; i++) {
                    int posToRead = start + 16 + (2 * i); // cell position
                    davisbaseTablesCatalog.seek(posToRead);
                    posToRead = davisbaseTablesCatalog.readShort();
//                    out.println("get meta table posToRead "+posToRead);
                    davisbaseTablesCatalog.seek(posToRead);
                    //cell header position reached
                    int payloadSize = davisbaseTablesCatalog.readShort();

                    davisbaseTablesCatalog.seek(posToRead + 6); // cell header = 6 bytes
//                    out.println("get meta table payloadSize "+payloadSize);
                    int numCols = davisbaseTablesCatalog.readByte();
//                    out.println("get meta table numCols "+numCols);
                    davisbaseTablesCatalog.seek(posToRead + 6 + 1);
                    davisbaseTablesCatalog.seek(posToRead + 6 + 1 + numCols);
                    String tableNamePresent = davisbaseTablesCatalog.readLine();
//                    out.println("get meta table tableNamePresent:"+tableNamePresent);
//                    out.println("get meta table input tableName:"+tableName);
                    if (tableName.equals(tableNamePresent)) {
//                        out.println("get meta match");
                        // reaching last byte of cell to see if table is dropped (true) or not
                        davisbaseTablesCatalog.seek((posToRead + 6 + payloadSize-1));
                        int isActive = davisbaseTablesCatalog.readByte();
//                        out.println("get meta table active value:"+isActive);
                        if (isActive== 1) {
                            if (getRootPage) {
                                davisbaseTablesCatalog.seek((posToRead + 6 + payloadSize) - 11);
                                rootPageOrRecCount = davisbaseTablesCatalog.readShort();
                            }
                            if (getRecordCount) {
                                davisbaseTablesCatalog.seek((posToRead + 6 + payloadSize)- 5);
                                rootPageOrRecCount = davisbaseTablesCatalog.readInt();
//                                out.println("get meta table rootPageOrRecCount "+rootPageOrRecCount);
                            }
                            break;
                        }
                    }
                }
                davisbaseTablesCatalog.seek(start + 6);
                rightSiblingPointer = davisbaseTablesCatalog.readInt();
                page = rightSiblingPointer;
            }
            davisbaseTablesCatalog.close();

        }
        catch (Exception e) {
            System.out.println(err_msg);
        }
        return rootPageOrRecCount;

    }


    static void initializeDataStore() {

        /** Create data directory at the current OS location to hold */

        try {


            File dataDir = new File("data");
            dataDir.mkdir();
            String[] oldTableFiles = dataDir.list();
            for (int i=0; i<oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }

            File catalogDir = new File("data/catalog");
            catalogDir.mkdir();

            File userDir = new File("data/user_data");
            userDir.mkdir();

        } catch (SecurityException se) {
            out.println("Error in creating data directory, try again");
        }

        /** Create davisbase_tables system catalog */
        try {
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            if (davisbaseTablesCatalog.length() == 0) {
                createPage(davisbaseTablesCatalog, true,-1);
            }
            davisbaseTablesCatalog.close();
        } catch (Exception e) {
            System.out.println("Unable to create the database_tables files");
        }

        /** Create davisbase_columns systems catalog */
        try {
            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
            if (davisbaseColumnsCatalog.length() == 0) {
                createPage(davisbaseColumnsCatalog, true,-1);
            }
            davisbaseColumnsCatalog.close();

            int rootPage = getfromMetaTable("davisbase_tables", true, false);
            if (rootPage == -1) {

                String[][] dbColsData = {               {"davisbase_tables", "rowid", "int","1","NO","YES","YES","1"},
                        {"davisbase_tables","table_name","text","2","NO","YES","NO","1"},
                        {"davisbase_tables","root_page","smallint","3","NO","NO","NO","1"},
                        {"davisbase_tables","last_id","int","4","NO","NO","NO","1"},
                        {"davisbase_tables","record_count","int","5","NO","NO","NO","1"},
                        {"davisbase_tables","isActive","tinyint","6","NO","NO","NO","1"},
                        {"davisbase_columns","rowid","int","1","NO","YES","YES","1"},
                        {"davisbase_columns","table_name","text","2","NO","YES","NO","1"},
                        {"davisbase_columns","column_name","text","3","NO","YES","NO","1"},
                        {"davisbase_columns","data_type","text","4","NO","NO","NO","1"},
                        {"davisbase_columns","ordinal_position","tinyint","5","NO","NO","NO","1"},
                        {"davisbase_columns","is_nullable","text","6","NO","NO","NO","1"},
                        {"davisbase_columns","is_unique","text","6","NO","NO","NO","1"},
                        {"davisbase_columns","is_PK","text","6","NO","NO","NO","1"},
                        {"davisbase_columns","isActive","int","7","NO","NO","NO","1"}
                };



                insertIntoMetaTable("davisbase_tables", (short) 0,0,0,(byte) 1);
                insertIntoMetaTable("davisbase_columns", (short) 0,0,0,(byte) 1);


                ArrayList<String> davisBaseColumnDTypes = new ArrayList<String>();
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("tinyint");
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("text");
                davisBaseColumnDTypes.add("tinyint");
                ArrayList<String> Row;
                for (int k = 0; k < dbColsData.length; k++) {
                    rootPage = getfromMetaTable("davisbase_columns", true, false);
                    Row = new ArrayList<String>();
                    Row.add(dbColsData[k][0]);
                    Row.add(dbColsData[k][1]);
                    Row.add(dbColsData[k][2]);
                    Row.add(dbColsData[k][3]);
                    Row.add(dbColsData[k][4]);
                    Row.add(dbColsData[k][5]);
                    Row.add(dbColsData[k][6]);
                    Row.add(dbColsData[k][7]);

                    insertIntoBtree("davisbase_columns", "data/catalog/davisbase_columns.tbl", rootPage, Row,
                            davisBaseColumnDTypes, Row);
                }
            }
        } catch (Exception e) {
            System.out.println("Unable to create meta-data files");
        }
    }

    private static void insertIntoBtree(String tableName, String tableFileName, int root_page, ArrayList<String> attribute_names, ArrayList<String> data_types, ArrayList<String> entered_values) {
        try {
//            out.println("entered insert b tree");
            RandomAccessFile tableFile = new RandomAccessFile(tableFileName, "rw");
//            out.println("root page :"+root_page);
            tableFile.seek(root_page * pageSize);


            Stack<Integer> interiorPages = new Stack<>();
            while (tableFile.readByte() != 13) //traversing till leaf page with page offset 0x0d
            {
                interiorPages.add((root_page * pageSize));
                tableFile.seek((root_page * pageSize) + 6);
                root_page = tableFile.readInt();
                tableFile.seek(root_page * pageSize);
            }

            int original_page_no = root_page;
            long seekIndex = original_page_no * pageSize;

            int rowid = getfromMetaTable(tableName, false, true) + 1;
//            out.println("rowid is : "+rowid);
            tableFile.seek(seekIndex + 2);
            int numRecords = tableFile.readShort() + 1;

            tableFile.seek(seekIndex + 4);
            int lastWritePos = tableFile.readShort();
//            out.println("last write Pos "+lastWritePos);
            int columns = attribute_names.size();
//            out.println("no of columns"+columns);

            ArrayList<Integer> codes = new ArrayList<Integer>();
            ArrayList<Integer> sizes = new ArrayList<Integer>();

            int payloadSize = 1 + columns;
//            out.println("payload size at start "+payloadSize);
//            out.println("values entered before" + entered_values);

            for (int i = 0; i < columns; i++) {
                if (data_types.get(i).equalsIgnoreCase("text")) {
//                    out.println("data type is text");
                    String buffer=entered_values.get(i)+"\n";
                    entered_values.set(i, buffer);
                    payloadSize += buffer.length();
//                    out.println("payload size now "+payloadSize);
                    sizes.add(i, entered_values.get(i).length());
                    codes.add(i, getDataTypeCode("text") + entered_values.get(i).length());
//                    out.println("entered value is "+entered_values.get(i));
                }
                else {
//                    out.println("entered value is "+entered_values.get(i));
//                    out.println("data type is: "+data_types.get(i));
//                    out.println("data type code is: "+getDataTypeCode(data_types.get(i).toLowerCase()));
//                    out.println("data type size is: "+getDataTypeSize(data_types.get(i).toLowerCase()));
                    codes.add(i, getDataTypeCode(data_types.get(i).toLowerCase()));
                    sizes.add(i, getDataTypeSize(data_types.get(i).toLowerCase()));
                    payloadSize += getDataTypeSize(data_types.get(i).toLowerCase());
//                    out.println("payload size now "+payloadSize);
                }
            }
//            out.println(entered_values);

//            out.println("payload size after "+payloadSize);

            int headerSize = 6;
            int record_size = headerSize + payloadSize;

            int cellToWrite = lastWritePos - record_size;
//            out.println("cellToWrite now "+cellToWrite);

            int pageHeaderPointer = (int) (seekIndex + 16 + (2 * (numRecords-1)));
//            out.println("pageHeaderPointer now "+pageHeaderPointer);


            if (pageHeaderPointer < cellToWrite) {
//                out.println("entered here in the leaf page");

                insertIntoLeafPage(tableFile, tableName, seekIndex, numRecords, rowid, cellToWrite,
                        pageHeaderPointer, payloadSize, columns, codes, entered_values, data_types);
            } else
            {
//                out.println("leaf page overflow, need to create another leaf page and possibly an interior page");
                int newRightSiblingPageNo = createPage(tableFile, true,-1);
//                out.println("came back from create page");
//                out.println("new sibling page no: "+newRightSiblingPageNo );
//                out.println("seekIndex :"+seekIndex);
                tableFile.seek(seekIndex + 2);
//                out.println("read at position 2 before"+tableFile.readShort());
                tableFile.seek(seekIndex + 6);
//                out.println("read at position 6 before"+tableFile.readInt());
                tableFile.seek(seekIndex + 6);
                tableFile.writeInt(newRightSiblingPageNo);
                tableFile.seek(seekIndex + 6);
//                out.println("read at position 6 after"+tableFile.readInt());
//
                int splitting_record = rowid;
//                out.println("splitting record: "+splitting_record);
                seekIndex = newRightSiblingPageNo * pageSize;
//                out.println("seekIndex : "+seekIndex);


                numRecords = 1;
                tableFile.seek(seekIndex + 4);
                lastWritePos = tableFile.readShort();
//                out.println("lastWritePos : "+lastWritePos);
                record_size = 6 + payloadSize;

                cellToWrite = lastWritePos - record_size;
//                out.println("cellToWrite : "+cellToWrite);
//
                pageHeaderPointer = (int) (seekIndex + 16 + (2 * (numRecords-1)));
//                out.println("pageHeaderPointer : "+pageHeaderPointer);
//                pageHeaderPointer = (int) (seekIndex + 16); //start area of first cell no

                insertIntoLeafPage(tableFile, tableName, seekIndex, numRecords, rowid, cellToWrite,
                        pageHeaderPointer, payloadSize, columns, codes, entered_values, data_types);

                boolean finished = false;

                while (!finished) {
                    if (interiorPages.isEmpty()) {
//                        out.println("no previous interior pages so came here");
                        int newInteriorPageNo = createPage(tableFile, false,-1);
//                        out.println("new parent page no: "+newInteriorPageNo);
                        seekIndex = newInteriorPageNo * pageSize;
//                        out.println("parent page seekIndex: "+seekIndex);
                        numRecords = 1;

                        tableFile.seek(seekIndex + 4);
                        lastWritePos = tableFile.readShort();
//                        out.println("parent page lastWritePos: "+lastWritePos);


                        record_size = 4 + 4;

                        cellToWrite = lastWritePos - record_size;
//                        out.println("parent page cellToWrite: "+cellToWrite);

                        pageHeaderPointer = (int) (seekIndex + 16 + (2 * (numRecords-1)));
//                        out.println("parent page pageHeaderPointer: "+pageHeaderPointer);

                        insertIntoInteriorPage(tableFile, seekIndex, numRecords, cellToWrite,pageHeaderPointer, splitting_record - 1, original_page_no, newRightSiblingPageNo);
//
                        // below: writing the parent interior page number to last leaf page and new leaf page at  position 0x0A (10)
                        tableFile.seek((original_page_no * pageSize)+10);
                        tableFile.writeInt(newInteriorPageNo);
                        tableFile.seek((newRightSiblingPageNo * pageSize)+10);
                        tableFile.writeInt(newInteriorPageNo);
                        //above: writing the parent interior page number to last leaf page and new leaf page at  position 0x0A (10)

                        RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                                "data/catalog/davisbase_tables.tbl", "rw");
                        updateMetaTable(davisbaseTablesCatalog, tableName, (int) newInteriorPageNo, true, false,false);

                        davisbaseTablesCatalog.close();
                        finished = true;

                    } else
                    {
//                        out.println("there is a previous interior page so came here");
                        seekIndex = interiorPages.pop();
                        //updating parent page number into the newly created leaf right sibling page
                        int parentInteriorPageNo= (int) (seekIndex/pageSize);
                        tableFile.seek((newRightSiblingPageNo * pageSize)+10);
                        tableFile.writeInt(parentInteriorPageNo);
                        //-------------------------------------------------------------------------

                        tableFile.seek(seekIndex + 2);
                        numRecords = tableFile.readShort() + 1;

                        tableFile.seek(seekIndex + 4);
                        lastWritePos = tableFile.readShort();

                        record_size = 4 + 4;

                        cellToWrite = lastWritePos - record_size;

                        pageHeaderPointer = (int) (seekIndex + 16 + (2 * (numRecords-1)));

                        if (pageHeaderPointer < cellToWrite) {
                            insertIntoInteriorPage(tableFile, seekIndex, numRecords, cellToWrite,
                                    pageHeaderPointer, splitting_record - 1, original_page_no, newRightSiblingPageNo);

                            finished = true;
                        } else {
//                            out.println("interior page overflow so came here");
                            original_page_no = (int) ((seekIndex / pageSize));
//                            out.println("left sibling interior page number "+ original_page_no);
                            int newInteriorPageNo = createPage(tableFile, false,-1);
//                            out.println("new interior page number "+ newInteriorPageNo);
//                            out.println("newRightSiblingPageNo  "+ newRightSiblingPageNo);

                            seekIndex = newInteriorPageNo * pageSize;
                            numRecords = 1;

                            tableFile.seek(seekIndex + 4);
                            lastWritePos = tableFile.readShort();

                            record_size = 4 + 4;

                            cellToWrite = lastWritePos - record_size;

                            pageHeaderPointer = (int) (seekIndex + 16 + (2 * (numRecords-1)));

                            insertIntoInteriorPage(tableFile, seekIndex, numRecords, cellToWrite,
                                    pageHeaderPointer, splitting_record - 1, original_page_no, newRightSiblingPageNo);

                            newRightSiblingPageNo = newInteriorPageNo;
                        }
                    }
                }

            }

            tableFile.close();
        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }


    private static void insertIntoInteriorPage(RandomAccessFile tableFile, long seekPointer,
                                               int numRecords, int cellToWrite, int lastPosOfPointers, int splitRecord, int leftChildPageNo,
                                               long new_page_no) {
//        out.println("entere insertInteriorPage");
//        out.println("seekPointer :"+ seekPointer);
//        out.println("numRecords :"+ numRecords);
//        out.println("cellToWrite :"+ cellToWrite);
//        out.println("lastPosOfPointers :"+ lastPosOfPointers);
//        out.println("splitRecord :"+ splitRecord);
//        out.println("leftChildPageNo :"+ leftChildPageNo);
//        out.println("new_page_no :"+ new_page_no);
        try {
            tableFile.seek(seekPointer + 2);
            tableFile.writeShort(numRecords);

            tableFile.seek(seekPointer + 4);
            tableFile.writeShort(cellToWrite);

            tableFile.seek(seekPointer + 6);
            tableFile.writeInt((int) new_page_no);

            tableFile.seek(lastPosOfPointers);
            tableFile.writeShort(cellToWrite);

            tableFile.seek(cellToWrite);
            tableFile.writeInt(splitRecord);
            tableFile.writeInt(leftChildPageNo);

        } catch (Exception e) {
            System.out.println(err_msg);
        }
    }

    private static void insertIntoLeafPage(RandomAccessFile tableFile, String tableName, long seekPointer,
                                           int numRecords, int rowid, int cellToWrite, int lastPosOfPointers, int payloadSize, int numCols,
                                           ArrayList<Integer> codes, ArrayList<String> entered_values, ArrayList<String> data_types) {
        try {
//            out.println("entered insert into leaf page");
//            out.println("tableName:"+tableName);
//            out.println("seek index:"+seekPointer);
//            out.println("cellToWrite:"+cellToWrite);
//            out.println("lastPosOfPointers:"+lastPosOfPointers);
//            out.println("payloadSize:"+payloadSize);
//            out.println("rowid:"+rowid);
//            out.println("numCols:"+numCols);
//            out.println("data codes:" + codes);
//            out.println("entered_values:"+entered_values);
//            out.println("data_types " + data_types);


            tableFile.seek(seekPointer + 2);
            tableFile.writeShort(numRecords);

            tableFile.seek(seekPointer + 4);
            tableFile.writeShort(cellToWrite);

//            tableFile.seek(lastPosOfPointers - 1);
            tableFile.seek(lastPosOfPointers);
            tableFile.writeShort(cellToWrite);

            tableFile.seek(cellToWrite);
            tableFile.writeShort(payloadSize);
            tableFile.writeInt(rowid);
            tableFile.writeByte(numCols);

            //record header
            for (int i = 0; i < numCols; i++) {
                if (entered_values.get(i).equals("null")) {
                    tableFile.writeByte(getDataTypeCode("null "));
                } else {
//                    out.println("code at "+i+" is :"+ codes.get(i));
                    tableFile.writeByte(codes.get(i));
                }
            }
            //record body
            String storeDatatype;
            for (int i = 0; i < numCols; i++) {

                if (entered_values.get(i).equals("null"))
                {
                    entered_values.set(i, "0");
                    continue;
                }
                storeDatatype=getStoredDataType(data_types.get(i).toLowerCase());
//                out.println("storedDatatype at i :"+i+" is : "+ storeDatatype);

                if (storeDatatype.equals("byte")) {
//                    out.println("parsing as byte");
                    tableFile.writeByte(Byte.parseByte(entered_values.get(i)));
                }
                if (storeDatatype.equals("short")) {
//                    out.println("parsing as short");
                    tableFile.writeShort(Short.parseShort(entered_values.get(i)));
                }
                if (storeDatatype.equals("int")) {
//                    out.println("parsing as int");
                    tableFile.writeInt(Integer.parseInt(entered_values.get(i)));
                }
                if (storeDatatype.equals("long")) {
//                    out.println("parsing as long");
                    if (data_types.get(i).equals("date") && !entered_values.get(i).equals("0")) {
                        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(entered_values.get(i));
                        tableFile.writeLong((date.getTime()));
                    } else if (data_types.get(i).equals("datetime") && !entered_values.get(i).equals("0")) {
                        Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(entered_values.get(i));
                        tableFile.writeLong((date.getTime()));
                    } else {
                        tableFile.writeLong(Long.parseLong(entered_values.get(i)));
                    }
                }
                if (storeDatatype.equals("float")) {
//                    out.println("parsing as float");
                    tableFile.writeFloat(Float.parseFloat(entered_values.get(i)));
                }
                if (storeDatatype.equals("double")) {
//                    out.println("parsing as double");
                    tableFile.writeDouble(Double.parseDouble(entered_values.get(i)));
                }
                if (storeDatatype.equals("line")) {
//                    out.println("parsing as line");
                    tableFile.writeBytes(entered_values.get(i));
                }
            }
//            out.println("tableName:"+tableName);
//            out.println("rowid:"+rowid);
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

            updateMetaTable(davisbaseTablesCatalog, tableName, rowid, false, true,false);
            davisbaseTablesCatalog.close();
        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }

    public static void getColumnMetaData(String tableName, ArrayList<String> attribute_names,
                                         ArrayList<String> data_types, ArrayList<String> constraint_isNull,ArrayList<String> constraint_isUnique,
                                         ArrayList<String> constraint_isPK) {
        try {
//            out.println("tableName "+tableName);
            RandomAccessFile davisbaseColumnsTable = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "r");
            int seek = 0;
            int right_child_pointer = 0;
            int page = 0;
            int records = 0;
            while (right_child_pointer != -1) {
                seek = (page * pageSize);
//                out.println("seek "+seek);
                davisbaseColumnsTable.seek(seek + 2);
                records = davisbaseColumnsTable.readShort();
//                out.println("records "+records);
                int code,size,bytesRead;
                String type;
                for (int i = 0; i < records; i++) {
                    bytesRead=0;
                    int posToRead = seek + 16 + (2 * i);
                    davisbaseColumnsTable.seek(posToRead);
                    posToRead = davisbaseColumnsTable.readShort();
//                    out.println("posToRead"+posToRead);

                    davisbaseColumnsTable.seek(posToRead);
                    int payload_size = davisbaseColumnsTable.readShort();
//                    out.println("payload_size"+payload_size);

                    davisbaseColumnsTable.seek(posToRead + 6);
                    int columns = davisbaseColumnsTable.readByte();
//                    out.println("columns"+columns);


                    davisbaseColumnsTable.seek(posToRead + 6 + 1 + columns);

                    String observedTableName = davisbaseColumnsTable.readLine();
//                    out.println("observedTableName: "+observedTableName);
//                    out.println("active position: "+(posToRead + payload_size + 6 - 1));

                    davisbaseColumnsTable.seek((posToRead + payload_size + 6 - 1));
                    int isActive = davisbaseColumnsTable.readByte();
//                    out.println("Active value: "+isActive);

                    if (tableName.equals(observedTableName) && (isActive == 1 || isActive==49)) {
                        for (int k = 0; k < (columns - 1); k++) {
                            davisbaseColumnsTable.seek(posToRead + 6 + 1 + k);
                            code = davisbaseColumnsTable.readByte();
//                            out.println("at"+k+" code is "+code);
                            type = getCodeDataType(code);
//                            out.println("at"+k+" type is "+type);
                            size = getDataTypeSize(type);
//                            out.println("at"+k+" size is "+size);

                            davisbaseColumnsTable.seek(posToRead + 6 + 1 + columns + bytesRead);
                            if (k == 1) {
                                String attribute_name=davisbaseColumnsTable.readLine();
                                attribute_names.add(attribute_name);
//                                out.println("attribute names now : "+attribute_names);

                            }
                            if (k == 2) {
                                data_types.add(davisbaseColumnsTable.readLine());
//                                out.println("data_types now : "+data_types);
                            }
                            if (k == 4) {
                                constraint_isNull.add(davisbaseColumnsTable.readLine());
//                                out.println("constraint_isNull now : "+constraint_isNull);
                            }
                            if (k == 5) {
                                constraint_isUnique.add(davisbaseColumnsTable.readLine());
//                                out.println("constraint_isUnique now : "+constraint_isUnique);
                            }
                            if (k == 6) {
                                constraint_isPK.add(davisbaseColumnsTable.readLine());
//                                out.println("constraint_isPK now : "+constraint_isPK);
                            }

                            if (type.equals("text")) {
                                bytesRead = (bytesRead + (code - 12));
                            } else {
                                bytesRead = bytesRead + size;
                            }
                        }
                    }
                }
                davisbaseColumnsTable.seek(seek + 6);
                right_child_pointer = davisbaseColumnsTable.readInt();
                page = right_child_pointer;
            }

            davisbaseColumnsTable.close();
        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }
    public static ArrayList<String> commandStringToTokenList (String command) {

        command=command.replace(";"," ; ");
        command=command.replace("';'"," ' ");
        command=command.replace("{"," { ");
        command=command.replace("}"," } ");
        command=command.replace("\n", " ");
        command=command.replace("\r", " ");
        command=command.replace(",", " , ");
        command=command.replace(" = ", "=");
        command=command.replace("=", " = ");
        command=command.replace(">", " >");
        command=command.replace("<", " <");
        command=command.replace("!", " !");
        command=command.replace("> =", ">=");
        command=command.replace("< =", "<=");
        command=command.replace("! =", "!=");
        command=command.replace("(", " ( ");
        command=command.replace(")", " ) ");
        command=command.replace("< >", "<>");
        command = command.trim().replaceAll(" +", " ");
        ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
        return tokenizedCommand;
    }
    public static void parseInsert(String command) {
//        out.println("entered parse Insert");
        ArrayList<String> insertTokens = commandStringToTokenList(command);
//        out.println("insert tokens "+ insertTokens);
        try {
            String tableName = insertTokens.get(2);
//            out.println("tableName: "+tableName);
            int root_page = -1;
            String tableFileName = null;
            if (tableName.equals("davisbase_tables")) {
                tableFileName = "data/catalog/" + tableName + ".tbl";
            } else {

                tableFileName = "data/user_data/" + tableName + ".tbl";
            }

            root_page = getfromMetaTable(tableName, true, false);
//            out.println("root_page: "+root_page);
            if (root_page == -1) {
                System.out.println("Table '" + tableName + "' doesnt exist");
                return;
            }

            ArrayList<String> attribute_names = new ArrayList<String>();
            ArrayList<String> data_types = new ArrayList<String>();
            ArrayList<String> constraint_isNull = new ArrayList<String>();
            ArrayList<String> constraint_isUnique = new ArrayList<String>();
            ArrayList<String> constraint_isPK = new ArrayList<String>();

            getColumnMetaData(tableName, attribute_names, data_types, constraint_isNull,constraint_isUnique,constraint_isPK);
            if(attribute_names.contains("rowid")){
                attribute_names.remove("rowid");
                data_types.remove(0);
                constraint_isNull.remove(0);
                constraint_isUnique.remove(0);
                constraint_isPK.remove(0);
            }
//            out.println("returned from getColumnMetaData ");
//            out.println("attribute_names "+attribute_names);
//            out.println("data_types "+data_types);
//            out.println("constraint_isNull "+constraint_isNull);
//            out.println("constraint_isUnique "+constraint_isUnique);
//            out.println("constraint_isPK "+constraint_isPK);
//            out.println("insert tokens "+ insertTokens);
//            out.println("tableName: "+tableName);

            ArrayList<String> entered_attribute_names = new ArrayList<String>();
            ArrayList<String> entered_values = new ArrayList<String>();
            ArrayList<String> temp_entered_values = new ArrayList<String>();


            if (insertTokens.get(3).equals("(")) {
//                out.println("insert command with mentioning col names");
                int j = 4;
                String end = insertTokens.get(j);
                while (!end.equals(")")) {
                    entered_attribute_names.add(insertTokens.get(j++));
                    end = insertTokens.get(j++);
                }
                j++; // values
                j++; // (
                end = insertTokens.get(j);
                while (!end.equals(")")) {
                    String[] tempArray = null;
                    String tempValue = insertTokens.get(j++);
                    if (tempValue.startsWith("'")) {
                        tempArray = tempValue.split("'");
                    }
                    if (tempValue.startsWith("\"")) {
                        tempArray = tempValue.split("\"");
                    }

                    if ((tempArray != null) && (tempArray.length > 1)) {
                        tempValue = tempArray[1];
                    } else if ((tempArray != null) && (tempArray.length == 1)) {
                        tempValue = tempArray[0];
                    }
                    temp_entered_values.add(tempValue);
                    end = insertTokens.get(j++);
                }
                if (entered_attribute_names.size() != temp_entered_values.size()) {
                    System.out.println("Field and value list must have same number of entries");
                    return;
                } else {
                    for (int r = 0; r < attribute_names.size(); r++) {
                        if (constraint_isNull.get(r).equalsIgnoreCase("No")) {
                            if (!entered_attribute_names.contains(attribute_names.get(r))) {
                                System.out.println("Not null attributes must be specified in the insert");
                                return;
                            }
                        }
                    }
                    for (int r = 0; r < entered_attribute_names.size(); r++) {
                        for (int s = 0; s < attribute_names.size(); s++) {
                            if (attribute_names.get(s).equals(entered_attribute_names.get(r))) {
                                if (constraint_isNull.get(s).equalsIgnoreCase("No")) {
                                    if (temp_entered_values.get(r).equalsIgnoreCase("null")) {
                                        System.out.println("Column '" + attribute_names.get(s) + "' cannot be null");
                                        return;
                                    }
                                }
                            }
                        }
                    }

                    for (int r = 0; r < attribute_names.size(); r++) {
                        boolean added = false;
                        for (int s = 0; s < entered_attribute_names.size(); s++) {
                            if (attribute_names.get(r).equals(entered_attribute_names.get(s))) {
                                entered_values.add(r, temp_entered_values.get(s));
                                added = true;
                            }
                        }
                        if (!added) {
                            entered_values.add(r, "null");
                        }
                    }
                    for (int r = 0; r < entered_attribute_names.size(); r++) {
                        if (!attribute_names.contains(entered_attribute_names.get(r))) {
                            System.out
                                    .println("Unknown column '" + entered_attribute_names.get(r) + "' in 'field list'");
                            return;
                        }
                    }

                }
            } else
            {
//                out.println("insert command withOUT mentioning col names");
                int j = 5;
                String end = insertTokens.get(j);
                while (!end.equals(")")) {
                    String[] tempArray = null;
                    String tempValue = insertTokens.get(j++);
//                    out.println("tempValue now: "+tempValue);
                    if (tempValue.startsWith("'")) {
//                        out.println("tempValue starts with ' ");
                        tempArray = tempValue.split("'");
                    }
                    if (tempValue.startsWith("\"")) {
//                        out.println("tempValue starts with \" ");
                        tempArray = tempValue.split("\"");
                    }

                    if ((tempArray != null) && (tempArray.length > 1)) {
                        tempValue = tempArray[1];
                    } else if ((tempArray != null) && (tempArray.length == 1)) {
                        tempValue = tempArray[0];
                    }
                    entered_values.add(tempValue);
//                    out.println("entered values now: "+entered_values);
                    end = insertTokens.get(j++);
                }

                if (entered_values.size() != attribute_names.size()) {
//                    out.println("attribute vs value size mismatch");
                    if (entered_values.size() < attribute_names.size()) {
                        for (int r = 0; r < entered_values.size(); r++) {
                            if (constraint_isNull.get(r).equalsIgnoreCase("No")) {
                                if (entered_values.get(r).equals("null")) {
                                    System.out.println("Column '" + attribute_names.get(r) + "' cannot be null");
                                    return;
                                }
                            }
                        }
                        int size = entered_values.size();
                        for (int r = size; r < attribute_names.size(); r++) {
                            if (constraint_isNull.get(r).equalsIgnoreCase("no")) {
                                out.println("Not null columns must be specified in the 'value list'");
                                return;
                            } else {
                                entered_values.add(r, "null");
                            }
                        }
                    } else {
                        System.out.println(err_msg);
                        return;
                    }

                } else
                {
//                    out.println("null check getting done");
                    for (int r = 0; r < attribute_names.size(); r++) {
                        if (constraint_isNull.get(r).equalsIgnoreCase("No")) {
                            if (entered_values.get(r).equalsIgnoreCase("null"))
                            {
                                out.println("Column '" + attribute_names.get(r) + "' cannot be null");
                                return;
                            }
                        }
                    }
                }

            }

            if (entered_values.size() != attribute_names.size()) {
                System.out.println(err_msg);
                return;
            }

//            out.println("tableName "+tableName );
//            out.println("tableFileName "+tableFileName );
//            out.println("root_page "+ root_page);
//            out.println("attribute_names "+attribute_names);
//            out.println("data_types "+data_types );
//            out.println("entered_values "+entered_values );

            insertIntoBtree(tableName, tableFileName, root_page, attribute_names, data_types, entered_values);

//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");

            out.println("Insertion Successful");

        } catch (Exception e) {
            System.out.println(err_msg);
        }
    }

    public static void parseCreateTable(String command) {

        ArrayList<String> commandTokens = commandStringToTokenList(command);
//        out.println("command tokens: "+commandTokens);
        try {
            String table_name = commandTokens.get(2);
            int root_page = getfromMetaTable(table_name, true, false);
//            out.println("root page: "+root_page);
            if (root_page > -1) {
                System.out.println("Table '" + table_name + "' already exists");
                return;
            }
            String tableFileName = commandTokens.get(2) + ".tbl";
//            out.println("tableFileName:"+tableFileName);

            ArrayList<String> attribute_names = new ArrayList<String>();
            ArrayList<String> data_types = new ArrayList<String>();
            ArrayList<String> constraint_isNull= new ArrayList<String>();
            ArrayList<String> constraint_isUnique = new ArrayList<String>();
            ArrayList<String> constraint_isPK = new ArrayList<String>();

            int i = 0;
            int j = 4;

            while (!commandTokens.get(j).contains(")"))
            {
                if(commandTokens.get(j).contains(","))
                {
                    j++;
                }
                String columnName=commandTokens.get(j++);
//                out.println("column name is " +columnName);
                attribute_names.add(i,columnName);
                String datatype=commandTokens.get(j).toLowerCase();
//                out.println("data type code is :" +getDataTypeCode(datatype));

                if(getDataTypeCode(commandTokens.get(j).toLowerCase()) == 12 && !commandTokens.get(j).equalsIgnoreCase("text"))
                {
                    out.println("Wrong data type: "+ commandTokens.get(j));
                    return;
                }
                else {
                    data_types.add(i, commandTokens.get(j++).toLowerCase());
                }

                if (!commandTokens.get(j).contains(",")) {
                    if (commandTokens.get(j).equalsIgnoreCase("not")) {
//                        out.println("found not null");
                        constraint_isNull.add(i, "NOT NULL");
                        j += 2;
                    }
                    if (commandTokens.get(j).equalsIgnoreCase("unique")) {
//                        out.println("found unique");
                        constraint_isUnique.add(i, "YES");
                        j++;

                    }
                    if (commandTokens.get(j).equalsIgnoreCase("PK")) {
//                        out.println("found PK");
                        constraint_isPK.add(i,"YES");
                        j++;
                    }

                }

                if(!commandTokens.get(j).equals(")") && !commandTokens.get(j).contains(","))
                {
                    out.println("Syntax error, columns not separated properly. see help for correct syntax");
                    return;
                }


                if(i>=constraint_isNull.size())
                {
                    constraint_isNull.add(i,"null");
                }
                if(i>=constraint_isUnique.size())
                {
                    constraint_isUnique.add(i,"NO");
                }
                if(i>=constraint_isPK.size())
                {
                    constraint_isPK.add(i,"NO");
                }
//                out.println("reached");
                i++;
            }

//            out.println("tableFileName: "+tableFileName);
//            out.println("attribute_names: "+attribute_names);
//            out.println("data_types: "+data_types);
//            out.println("constraints null: " +constraint_isNull);
//            out.println("constraints unique: " +constraint_isUnique);
//            out.println("constraints PK: " +constraint_isPK);



            RandomAccessFile tableFile = new RandomAccessFile("data/user_data/" + tableFileName, "rw");
            tableFile.setLength(0);
            createPage(tableFile, true,-1);
            tableFile.close();
            parseInsert("insert into davisbase_tables values ( " + table_name + " , " + 0 + " , " + 0 + " , " + 0 + " , " + 1 + " )");

            ArrayList<String> singleRow;
            ArrayList<String> dTypes;
            for (int k = 0; k < i; k++) {
                root_page = getfromMetaTable("davisbase_columns", true, false);
                singleRow = new ArrayList<String>();
                singleRow.add(table_name);
                singleRow.add(attribute_names.get(k));
                singleRow.add(data_types.get(k));
                singleRow.add(String.valueOf(k + 1));
                singleRow.add(constraint_isNull.get(k).equalsIgnoreCase("null") ? "YES" : "NO");
                singleRow.add(constraint_isUnique.get(k).equalsIgnoreCase("YES") ? "YES" : "NO");
                singleRow.add(constraint_isPK.get(k).equalsIgnoreCase("YES") ? "YES" : "NO");
                singleRow.add("1");

                dTypes = new ArrayList<String>();
                dTypes.add("text");
                dTypes.add("text");
                dTypes.add("text");
                dTypes.add("tinyint");
                dTypes.add("text");
                dTypes.add("text");
                dTypes.add("text");
                dTypes.add("tinyint");

                insertIntoBtree("davisbase_columns", "data/catalog/davisbase_columns.tbl", root_page, singleRow, dTypes,
                        singleRow);
//                out.println("inserted in btree");
            }
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
            out.println("Creation Successful");

        } catch (Exception e) {
            System.out.println(err_msg);
            return;
        }


    }
    public static void parseQuery(String command) {

        ArrayList<String> selectTokens = commandStringToTokenList(command);
//        out.println("selectTokens :" + selectTokens);
        ArrayList<String> variables = new ArrayList<String>();
        boolean includeRowID=false;
        boolean projectAll = false;
        boolean whereClauseIncluded = false;
        String whereAttribute_1 = null;
        String whereOperator_1 = null;
        ArrayList<String> whereValue_1 = new ArrayList<String>();
        String whereConnectString=null; // AND / OR
        String whereAttribute_2 = null;
        String whereOperator_2= null;
        ArrayList<String> whereValue_2 = new ArrayList<String>();
        int i=1;
        String table_name;
        try {
            if (selectTokens.get(i).equals("*")) {
//                out.println("entered query");

                if (!selectTokens.get(i+1).equalsIgnoreCase("from")) {
                    System.out.println(err_msg);
                    return;
                }

                table_name = selectTokens.get(i+2);
//                out.println("table_name:" + table_name);

                projectAll = true;
                i=4;

            }
            else
            {
                projectAll = false;
                while (!selectTokens.get(i).equalsIgnoreCase("from")) {
                    if (!selectTokens.get(i).equals(","))
                    {
                        variables.add(selectTokens.get(i++));
                    }
                    else
                    {
                        i++;
                    }
                }
                i++;
                table_name=selectTokens.get(i++);
            }

            while( !selectTokens.get(i).equals(";") )
            {
//                out.println("checking for where cases");
                if (selectTokens.get(i).equalsIgnoreCase("where")) {
                    i++;
                    whereClauseIncluded = true;
                    whereAttribute_1 = selectTokens.get(i++).trim();
                    whereOperator_1 = selectTokens.get(i++).trim();


                    if ((whereOperator_1.equalsIgnoreCase("is") && selectTokens.get(i).equalsIgnoreCase("not")) ||
                            whereOperator_1.equalsIgnoreCase("not") && selectTokens.get(i).equalsIgnoreCase("in")) {
                        whereOperator_1 += " " + selectTokens.get(i++);
                    }
//                    out.println("where 1 attribute :" +whereAttribute_1);
//                    out.println("where 1 operator  :" +whereOperator_1);



                    while(!(selectTokens.get(i).equalsIgnoreCase("and") || selectTokens.get(i).equalsIgnoreCase("or") || selectTokens.get(i).equalsIgnoreCase(";") ))
                    {
                        if (!(selectTokens.get(i).equals(",") || selectTokens.get(i).equals("'") ||selectTokens.get(i).equals("{")||selectTokens.get(i).equals("}")))
                        {
                            whereValue_1.add(selectTokens.get(i).replace("'", ""));
                        }
                        i++;

                    }

//                    out.println("where 1 value  :" +whereValue_1);

                    if(!selectTokens.get(i).equalsIgnoreCase(";") && (selectTokens.get(i).equalsIgnoreCase("and") || selectTokens.get(i).equalsIgnoreCase("or")))
                    {
//                        out.println("entered second where clause");
                        whereConnectString=selectTokens.get(i++);
                        whereAttribute_2 = selectTokens.get(i++);
                        whereOperator_2 = selectTokens.get(i++);
                        if ((whereOperator_2.equalsIgnoreCase("is") && selectTokens.get(i).equalsIgnoreCase("not")) ||
                                whereOperator_2.equalsIgnoreCase("not") && selectTokens.get(i).equalsIgnoreCase("in")) {
                            whereOperator_2 += " " + selectTokens.get(i++);
                        }
                        while(!selectTokens.get(i).equalsIgnoreCase(";"))
                        {

                            if (!(selectTokens.get(i).equals(",")  || selectTokens.get(i).equals("'") ||selectTokens.get(i).equals("{")||selectTokens.get(i).equals("}")))
                            {
                                whereValue_2.add(selectTokens.get(i).replace("'", ""));
                            }
                            i++;
                        }

                    }
//                    out.println("at position i "+i +" value is "+ selectTokens.get(i));


                }
            }


            if(whereClauseIncluded) {
                if ((whereOperator_1.contains("=") && whereValue_1.size() > 1) || (whereValue_2.size() > 1 && whereOperator_2.contains("=") )) {
                    out.println("multiple values can't be matched with '=' operator in where, check help for syntax");
                    return;
                }
            }


            String table_file_name = null;
            if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
                table_file_name = "data/catalog/" + table_name + ".tbl";
            } else {
                table_file_name = "data/user_data/" + table_name + ".tbl";
            }
//            out.println(table_name);
//            out.println(table_file_name);

//            if(!projectAll) {
//                out.println("variable list: \n");
//                for (int j = 0; j < variables.size(); j++) {
//                    out.println(variables.get(j));
//                }
//            }
//            if(whereClauseIncluded)
//            {
////                out.println("where attr 1: "+whereAttribute_1);
////                out.println("where ops 1: "+whereOperator_1);
////                out.println("where values:");
////                for (int j = 0; j < whereValue_1.size(); j++) {
////                    out.println(whereValue_1.get(j));
////                }
//                if(whereConnectString !=null) {
//
////                out.println("where connect string size :" + whereConnectString.length());
//
//                    if (whereConnectString.equalsIgnoreCase("and") || whereConnectString.equalsIgnoreCase("or")) {
//                        out.println("where connect string: " + whereConnectString);
//                        out.println("where attr 2: " + whereAttribute_2);
//                        out.println("where ops 2: " + whereOperator_2);
//                        out.println("second where values:");
//                        for (int j = 0; j < whereValue_2.size(); j++) {
//                            out.println(whereValue_2.get(j));
//                        }
//                    }
//                }
//                out.println("reached");
//
//            }


            ArrayList<ArrayList<String> > Table1 = new ArrayList<ArrayList<String>>();
            ArrayList<ArrayList<String> > Table2 = new ArrayList<ArrayList<String>>();
            ArrayList<ArrayList<String>> FinalTable = new ArrayList<ArrayList<String>>();

//            out.println("variables" + variables);
            if(variables.contains("rowid")) {
                includeRowID=true;
            }

            if(!whereClauseIncluded) {
                read_table(table_name, table_file_name, projectAll, whereClauseIncluded, variables, "",
                        "", "", false, Table1);
                FinalTable.addAll(Table1);
            }
            else
            {
                if(whereConnectString ==null) {
                    read_table(table_name, table_file_name, projectAll, whereClauseIncluded, variables, whereAttribute_1,
                            whereOperator_1, whereValue_1.get(0), (whereClauseIncluded && (whereAttribute_1 != null) && whereAttribute_1.equals("rowid")), Table1);
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("Table : "+Table1);
                    FinalTable.addAll(Table1);
                }
                else {
                    read_table(table_name, table_file_name, projectAll, whereClauseIncluded, variables, whereAttribute_1,
                            whereOperator_1, whereValue_1.get(0), (whereClauseIncluded && (whereAttribute_1 != null) && whereAttribute_1.equals("rowid")), Table1);

                    read_table(table_name, table_file_name, projectAll, whereClauseIncluded, variables, whereAttribute_2,
                            whereOperator_2, whereValue_2.get(0), (whereClauseIncluded && (whereAttribute_2 != null) && whereAttribute_2.equals("rowid")), Table2);
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("Table 1: "+ Table1);
//                    out.println("Table 2: "+ Table2);
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");

                    ArrayList<ArrayList<String>> SmallerTable = new ArrayList<ArrayList<String>>();
                    ArrayList<ArrayList<String>> LargerTable = new ArrayList<ArrayList<String>>();


                    int s1 = 0, s2 = 0, f = 0;
                    FinalTable.add(Table1.get(0)); //adding headers once to final table
                    Table1.remove(0); // removing headers
                    Table2.remove(0); // removing headers
                    if (Table1.size() <= Table2.size()) {
                        SmallerTable = Table1;
                        LargerTable = Table2;
                    } else {
                        SmallerTable = Table2;
                        LargerTable = Table1;
                    }
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                    out.println("Smaller Table \n "+ SmallerTable);
//                    out.println("Larger Table \n "+ LargerTable);
//                    out.println("-----------------\n\n\n\n\n\n\n\n\n\n");

                    if (whereConnectString.equalsIgnoreCase("AND")) {
//                        out.println("entered and");
                        while (s1 < SmallerTable.size()) {
//                            out.println("rowid to check :" +SmallerTable.get(s1).get(0));
                            while (s2 < LargerTable.size()) {
//                                out.println("rowid in larger one :" +LargerTable.get(s2).get(0));

                                if (SmallerTable.get(s1).get(0).equals(LargerTable.get(s2).get(0))) {
//                                    out.println("match found");
                                    FinalTable.add(SmallerTable.get(s1));
                                    SmallerTable.remove(s1);
                                    break;
                                }
                                s2++;
                                out.println();
                            }
                            s2 = 0;
                            s1++;
                        }
//                        out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                        out.println("Final Table "+ FinalTable);
//                        out.println("-----------------\n\n\n\n\n\n\n\n\n\n");

                    } else {
//                        out.println("entered or");
                        while (s1 < SmallerTable.size()) {
                            while (s2 < LargerTable.size()) {
                                if (SmallerTable.get(s1).get(0).equals(LargerTable.get(s2).get(0))) {
                                    SmallerTable.remove(s1);
                                    break;
                                }
                                s2++;
                            }
                            s2 = 0;
                            s1++;
                        }
                        FinalTable.addAll(SmallerTable);
                        FinalTable.addAll(LargerTable);
//                        out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                        out.println("Final Table "+ FinalTable);
//                        out.println("-----------------\n\n\n\n\n\n\n\n\n\n");

                    }
//                    out.println("Table" + FinalTable);
                }



            }
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
            ArrayList<String> PrintRow =  new ArrayList<String>();
            String line = null;
            boolean header=true;

            for(int x=0;x<FinalTable.size();x++)
            {
                if(!includeRowID)
                {
                    FinalTable.get(x).remove(0);
                }
                PrintRow=FinalTable.get(x);
                if(header) {
                    line = line("%-" + ((PrintRow.size() * 20) / PrintRow.size()) + "s", PrintRow.size()) + "\n";
                    System.out.format(line,PrintRow.toArray());
                    System.out.println(line("-", (PrintRow.size() * 20)));
                    header=false;
                }
                else {
                    System.out.format(line, PrintRow.toArray());
                }

            }

        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }
    public static String line(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }

    private static void read_table(String table_name, String table_file_name, boolean projectAll,
                                   boolean whereClauseIncluded, ArrayList<String> projectiles, String whereAttribute, String whereOperator,
                                   String whereValue, boolean rowidSearch,ArrayList<ArrayList<String> > Table) {
        try {
//            out.println("entered read table");
//            out.println("table name: "+table_name );

            if(!projectAll && !projectiles.contains("rowid")){
//                out.println("here i added rowid");
                projectiles.add(0,"rowid");
            }

//            out.println("projectiles: "+ projectiles);


            int root_page = getfromMetaTable(table_name, true, false);
//            out.println("root_page: "+root_page);
            if (root_page == -1) {
                System.out.println("Table '" + table_name + "' doesnt exist");
                return;
            }

            ArrayList<String> attribute_names = new ArrayList<String>();
            ArrayList<String> data_types = new ArrayList<String>();
            ArrayList<String> constraint_isNull = new ArrayList<String>();
            ArrayList<String> constraint_isUnique = new ArrayList<String>();
            ArrayList<String> constraint_isPK = new ArrayList<String>();

            if(!(table_name.equalsIgnoreCase("davisbase_tables") || table_name.equalsIgnoreCase("davisbase_columns")))
            {
//                out.println("adding row id to attribute names");
                attribute_names.add("rowid");
            }

            data_types.add("int");
            constraint_isNull.add("NO");
            constraint_isUnique.add("YES");
            constraint_isPK.add("YES");



            getColumnMetaData(table_name, attribute_names, data_types, constraint_isNull,constraint_isUnique,constraint_isPK);
//            out.println("return from getColumnMetaData");
//            out.println("the file table name: "+table_file_name );

            if(whereClauseIncluded) {
                if ((whereAttribute != null) && !attribute_names.contains(whereAttribute)) {
                    System.out.println("Column '" + whereAttribute + "' in 'where' condition is not present in table");
                    return;
                }
            }

            RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "r");


            boolean printingFirstTime = true;
            boolean whereValueChanged = false;

            int seek = 0;
            int rightSibling = 0;
            int startPage = 0;
            int records = 0;
            int endPage = Integer.MAX_VALUE - 1;
            int row_count = 0;

            if (rowidSearch) {
//                out.println("row id wise search");
                startPage = root_page;
                seek = (int) (startPage * pageSize);
                tableFile.seek(seek);
                int prevRowId,nextRowId,prevPageNo;

                while (tableFile.readByte() != 13) {
                    tableFile.seek(seek + 2);
                    records = tableFile.readShort();

                    int posToRead = seek + 16 + (2 * (records-1)); // last written cell ( top most)
                    tableFile.seek(posToRead);
                    posToRead = tableFile.readShort();
                    tableFile.seek(posToRead);
                    prevRowId =nextRowId=tableFile.readInt();
                    if (Integer.parseInt(whereValue) <= prevRowId) {
                        prevPageNo = tableFile.readInt();
                        while ((records > 1) && (Integer.parseInt(whereValue) < nextRowId)) {
                            records--;
                            posToRead = seek + 16 + (2 * (records-1));
                            tableFile.seek(posToRead);
                            posToRead = tableFile.readShort();
                            tableFile.seek(posToRead);
                            prevRowId = tableFile.readInt();
                            prevPageNo = tableFile.readInt();

                            if (records != 1) {
                                records--;
                                posToRead = seek + 16 + (2 * (records-1));
                                tableFile.seek(posToRead);
                                posToRead = tableFile.readShort();
                                tableFile.seek(posToRead);
                                nextRowId = tableFile.readInt();
                                records++;
                            }
                        }
                        startPage = prevPageNo;
                    } else {
                        tableFile.seek(seek + 6);
                        startPage = tableFile.readInt();
                    }
                    seek = (int) (startPage * pageSize);
                    tableFile.seek(seek);
                }
                if (whereOperator.contains("<")) {
                    endPage = startPage;
                    startPage = 0;
                }
                if (whereOperator.equals("=")) {
                    endPage = startPage ;
                }
            }

//            out.println("start page"+startPage);
//            out.println("end page"+endPage);
            while ((rightSibling != -1) && (rightSibling < (endPage + 1))) {
//                out.println("entered searching part");
                seek = (int) (startPage * pageSize);
//                out.println("seek value now: "+seek);
                //---------------------------
                tableFile.seek(seek + 6);
                rightSibling = tableFile.readInt();
                startPage = rightSibling;
                //---------------------------

                tableFile.seek(seek + 2);
                records = tableFile.readShort();
//                out.println("records value now: "+records);

                for (int i = 0; i < records; i++) {
                    ArrayList<String> values = new ArrayList<String>();
                    int posToRead = seek + 16 + (2 * i);
                    tableFile.seek(posToRead);
                    posToRead = tableFile.readShort();
//                    out.println("postoread value now: "+posToRead);
                    if(posToRead == 0)
                    {
//                        out.println("record deleted ");
                        continue;
                    }

                    tableFile.seek(posToRead + 6);
                    int columns = tableFile.readByte();
//                    out.println("columns value now: "+columns);

                    String[] testValues = new String[columns + 1];
                    tableFile.seek(posToRead + 2);

                    testValues[0] = String.valueOf(tableFile.readInt());
//                    out.println("testValues  now: "+testValues[0]);

                    int bytesRead = 0;

                    for (int k = 0; k < columns; k++) {
//                        out.println("for k= "+k);
//                        out.println("bytesRead till now :"+ bytesRead);
                        tableFile.seek(posToRead + 6 + 1 + k);

                        int code = tableFile.readByte();
//                        out.println("code : "+code);

                        String type = getCodeDataType(code);
//                        out.println("type : "+type);
                        tableFile.seek(posToRead + 6 + 1 + columns + bytesRead);
                        String method;

                        if (getDataTypeCode(type) ==0) {
                            method = "null";
                        } else {
                            method = getStoredDataType(type);
                        }
//                        out.println("method : "+method);

                        switch(method)
                        {
                            case "null":
                                testValues[k + 1] = "null";
                                break;
                            case "byte":
//                                out.println("entered byte");
                                testValues[k + 1] = String.valueOf(tableFile.readByte());
                                break;
                            case "short":
//                                out.println("entered short");
                                testValues[k + 1] = String.valueOf(tableFile.readShort());
                                break;
                            case "int":
                            case "integer":
//                                out.println("entered integer");
                                testValues[k + 1] = String.valueOf(tableFile.readInt());
                                break;
                            case "long":
//                                out.println("entered long");
                                testValues[k + 1] = String.valueOf(tableFile.readLong());
                                break;
                            case "float":
//                                out.println("entered float");
                                testValues[k + 1] = String.valueOf(tableFile.readFloat());
                                break;
                            case "double":
//                                out.println("entered double");
                                testValues[k + 1] = String.valueOf(tableFile.readDouble());
                                break;
                            default:
//                                out.println("entered default");
                                String temp = String.valueOf(tableFile.readLine());
                                if (temp.equals("")) {
                                    testValues[k + 1] = "null";
                                } else {
                                    testValues[k + 1] = temp;
                                }
                                break;
                        }

                        if (type.equals("text")) {
//                            out.println("entered here");
                            bytesRead = (bytesRead + (code - 12));

                        } else {
                            bytesRead = bytesRead + getDataTypeSize(type);
                        }
                    }
                    boolean include = true;

                    if (whereClauseIncluded) {
                        include = false;
                        for (int l = 0; l < attribute_names.size(); l++) {
                            if (attribute_names.get(l).equalsIgnoreCase(whereAttribute)) {
                                if (data_types.get(l).equalsIgnoreCase("date") && !whereValueChanged
                                        && !whereValue.equalsIgnoreCase("null")) {
                                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(whereValue);
                                    whereValue=String.valueOf(date.getTime());

                                    whereValueChanged = true;
                                } else if (data_types.get(l).equalsIgnoreCase("datetime") && !whereValueChanged
                                        && !whereValue.equalsIgnoreCase("null")) {
                                    Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(whereValue);
                                    whereValue=String.valueOf(date.getTime());
                                    whereValueChanged = true;
                                }
                                switch (whereOperator.toLowerCase()) {
                                    case "in":
                                    case "=":
                                    {
                                        if (data_types.get(l).equalsIgnoreCase(("text"))) {
                                            if (whereValue.equalsIgnoreCase("null")) {
                                                include = false;
                                            } else {
                                                include = testValues[l].equalsIgnoreCase(whereValue);
                                            }
                                        } else {
                                            if (whereValue.equalsIgnoreCase("null")) {
                                                include = false;
                                            } else if (testValues[l].equalsIgnoreCase("null")) {
                                                include = false;
                                            } else {
                                                include = Double.valueOf(testValues[l]).equals(Double.valueOf(whereValue));
                                            }
                                        }
                                        break;
                                    }
                                    case "is": {
                                        if (whereValue.equalsIgnoreCase("null")) {
                                            include = testValues[l].equals("null");
                                        }
                                        break;
                                    }

                                    case "<": {
                                        if (!testValues[l].equalsIgnoreCase("null")) {
                                            if (data_types.get(l).equalsIgnoreCase("text")) {
                                                include = testValues[l].compareTo(whereValue) < 0;
                                            } else {
                                                if (!whereValue.equalsIgnoreCase("null")) {
                                                    include = Double.valueOf(testValues[l]).compareTo(Double.valueOf(whereValue)) < 0;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case "<=": {
                                        if (!testValues[l].equalsIgnoreCase("null")) {
                                            if (data_types.get(l).equalsIgnoreCase("text")) {
                                                include = testValues[l].compareTo(whereValue) <= 0;
                                            } else {
                                                if (!whereValue.equalsIgnoreCase("null")) {
                                                    include = Double.valueOf(testValues[l]).compareTo(Double.valueOf(whereValue)) <= 0;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case ">": {
                                        if (!testValues[l].equalsIgnoreCase("null")) {
                                            if (data_types.get(l).equalsIgnoreCase("text")) {
                                                include = testValues[l].compareTo(whereValue) > 0;
                                            } else {
                                                if (!whereValue.equalsIgnoreCase("null")) {
                                                    include = Double.valueOf(testValues[l]).compareTo(Double.valueOf(whereValue)) > 0;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case ">=": {
                                        if (!testValues[l].equalsIgnoreCase("null")) {
                                            if (data_types.get(l).equalsIgnoreCase("text")) {
                                                include = testValues[l].compareTo(whereValue) >= 0;
                                            } else {
                                                if (!whereValue.equalsIgnoreCase("null")) {
                                                    include = Double.valueOf(testValues[l]).compareTo(Double.valueOf(whereValue)) >= 0;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case "<>":
                                    case "!=": {
                                        if (data_types.get(l).equalsIgnoreCase("text")) {
                                            if (whereValue.equalsIgnoreCase("null")) {
                                                include = false;
                                            } else {
                                                include = !testValues[l].equalsIgnoreCase(whereValue);
                                            }
                                        } else {
                                            if (whereValue.equalsIgnoreCase("null")) {
                                                include = false;
                                            } else if (testValues[l].equalsIgnoreCase("null")) {
                                                include = false;
                                            } else {
                                                include = Double.valueOf(testValues[l]).compareTo(Double.valueOf(whereValue)) != 0;
                                            }
                                        }
                                        break;
                                    }
                                    case "is not": {
                                        if (whereValue.equalsIgnoreCase("null")) {
                                            include = !testValues[l].equalsIgnoreCase("null");
                                        }
                                        break;
                                    }
                                    case "like": {
                                        if (data_types.get(l).equals("text") && !testValues[l].equals("null")) {
                                            String tempValue = whereValue;
                                            if (tempValue.contains("%")) {
                                                tempValue = tempValue.toLowerCase();
                                                tempValue = tempValue.replace("_", ".");
                                                tempValue = tempValue.replace("%", ".*");
                                                testValues[l] = testValues[l].toLowerCase();
                                                include = testValues[l].matches(tempValue);
                                            } else {
                                                include = testValues[l].equals(whereValue);
                                            }
                                        }
                                        break;
                                    }
                                    default: {
                                        System.out.println(err_msg);
                                        return;
                                    }
                                }
                            }
                        }
                    }

//                    out.println("entered update table zone");
//                    out.println("Table before: "+ Table);
                    if (projectAll && printingFirstTime) {
//                        out.println("project all ");
//                        out.println("attribute_names: "+ attribute_names);
                        Table.add(row_count,attribute_names);
                        printingFirstTime = false;
                    }
                    else if (printingFirstTime) {
                        for (int s = 0; s < projectiles.size(); s++) {
                            if (!attribute_names.contains(projectiles.get(s))) {
                                System.out.println("Unknown column '" + projectiles.get(s) + "' in 'field list' ");
                                tableFile.close();
                                return;
                            }
                        }
                        System.out.println();
//                        line = line("%-" + ((projectiles.size() * 20) / projectiles.size()) + "s", projectiles.size())
//                                + "\n";
//                        System.out.format(line, projectiles.toArray());
//                        System.out.println(line("-", (projectiles.size() * 20)));
                        Table.add(row_count,projectiles);
                        printingFirstTime = false;
                    }

                    if (include) {
                        for (int l = 0; l < attribute_names.size(); l++) {
                            if (data_types.get(l).equals("date") && (testValues[l] != "null")) {
                                Date date = new Date(Long.parseLong(testValues[l]));
                                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                                testValues[l] = df.format(date);
                            } else if (data_types.get(l).equals("datetime") && (testValues[l] != "null")) {
                                Date date = new Date(Long.parseLong(testValues[l]));
                                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
                                testValues[l] = df.format(date);
                            }
                        }
                        if (projectAll) {
//                            System.out.format(line, (Object[]) testValues);
                            row_count++;
                            Table.add(row_count, new ArrayList<>(Arrays.asList(testValues)));
                        } else {
                            for (int k = 0; k < projectiles.size(); k++) {
                                for (int l = 0; l < attribute_names.size(); l++) {
                                    if (attribute_names.get(l).equals(projectiles.get(k))) {
                                        values.add(testValues[l]);
                                    }
                                }
                            }
//                            System.out.format(line, values.toArray());
                            row_count++;
                            Table.add(row_count, values);
                        }
                    }
//                    out.println("Table after " + Table);
                }

            }
//            System.out.println();
//            System.out.println(row_count + " row(s) returned");
//            System.out.println();

            tableFile.close();
        } catch (Exception e) {
            if (e instanceof FileNotFoundException) {
                System.out.println("Table '" + table_name + "' doesnt exist");
            } else {
                System.out.println(err_msg);
            }
        }
    }
    private static void droptable(String tableName)
    {
        try {
            //first delete records
            String table_file_name = null;
            if (tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")) {
                out.println("can't delete meta table , it will corrupt database");
                return;
            }
            else {
                table_file_name = "data/user_data/" + tableName + ".tbl";
            }
//            out.println(tableName);
//            out.println(table_file_name);

            delete_records(tableName, table_file_name, true, "rowid",
                    "=", "1",false,true);

            //then turn active flag off in meta table davisbase_tables
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            updateMetaTable(davisbaseTablesCatalog, tableName, 0, false, false,true);
            //then turn active flag off in meta table columns
            RandomAccessFile davisbaseColumnsCatalog=new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
            updateMetaTable(davisbaseColumnsCatalog, tableName, 0, false, false,true);

            Path fileToDeletePath = Paths.get(table_file_name);
            try {
                Files.delete(fileToDeletePath);
            }
            catch(Exception e)
            {
                out.println("file not found");
            }
//            out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
            out.println("Drop Successful");

        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }

    public static void parseDelete(String command) {

        ArrayList<String> selectTokens = commandStringToTokenList(command);
//        out.println("selectTokens :" + selectTokens);
        ArrayList<String> variables = new ArrayList<String>();
        boolean includeRowID=false;
        boolean projectAll = false;
        boolean whereClauseIncluded = false;
        String whereAttribute_1 = null;
        String whereOperator_1 = null;
        ArrayList<String> whereValue_1 = new ArrayList<String>();
        String whereConnectString=null; // AND / OR
        String whereAttribute_2 = null;
        String whereOperator_2= null;
        ArrayList<String> whereValue_2 = new ArrayList<String>();
        int i=2;
        String table_name;
        try {


            table_name=selectTokens.get(i++);
            while( !selectTokens.get(i).equals(";") )
            {
//                out.println("checking for where cases");
                if (selectTokens.get(i).equalsIgnoreCase("where")) {
                    i++;
                    whereClauseIncluded = true;
                    whereAttribute_1 = selectTokens.get(i++).trim();
                    whereOperator_1 = selectTokens.get(i++).trim();


                    if ((whereOperator_1.equalsIgnoreCase("is") && selectTokens.get(i).equalsIgnoreCase("not")) ||
                            whereOperator_1.equalsIgnoreCase("not") && selectTokens.get(i).equalsIgnoreCase("in")) {
                        whereOperator_1 += " " + selectTokens.get(i++);
                    }
//                    out.println("where 1 attribute :" +whereAttribute_1);
//                    out.println("where 1 operator  :" +whereOperator_1);



                    while(!(selectTokens.get(i).equalsIgnoreCase("and") || selectTokens.get(i).equalsIgnoreCase("or") || selectTokens.get(i).equalsIgnoreCase(";") ))
                    {
                        if (!(selectTokens.get(i).equals(",") || selectTokens.get(i).equals("'") ||selectTokens.get(i).equals("{")||selectTokens.get(i).equals("}")))
                        {
                            whereValue_1.add(selectTokens.get(i).replace("'", ""));
                        }
                        i++;

                    }

//                    out.println("where 1 value  :" +whereValue_1);

                    if(!selectTokens.get(i).equalsIgnoreCase(";") && (selectTokens.get(i).equalsIgnoreCase("and") || selectTokens.get(i).equalsIgnoreCase("or")))
                    {
//                        out.println("entered second where clause");
                        whereConnectString=selectTokens.get(i++);
                        whereAttribute_2 = selectTokens.get(i++);
                        whereOperator_2 = selectTokens.get(i++);
                        if ((whereOperator_2.equalsIgnoreCase("is") && selectTokens.get(i).equalsIgnoreCase("not")) ||
                                whereOperator_2.equalsIgnoreCase("not") && selectTokens.get(i).equalsIgnoreCase("in")) {
                            whereOperator_2 += " " + selectTokens.get(i++);
                        }
                        while(!selectTokens.get(i).equalsIgnoreCase(";"))
                        {

                            if (!(selectTokens.get(i).equals(",")  || selectTokens.get(i).equals("'") ||selectTokens.get(i).equals("{")||selectTokens.get(i).equals("}")))
                            {
                                whereValue_2.add(selectTokens.get(i).replace("'", ""));
                            }
                            i++;
                        }

                    }
//                    out.println("at position i "+i +" value is "+ selectTokens.get(i));


                }
            }

            if(!whereClauseIncluded)
            {
                out.println("Can't delete records without a where condition. To delete all records, drop the table");
                return;
            }
            if(whereClauseIncluded) {


                if ((whereOperator_1.contains("=") && whereValue_1.size() > 1) || (whereValue_2.size() > 1 && whereOperator_2.contains("=") )) {
                    out.println("multiple values can't be matched with '=' operator in where, check help for syntax");
                    return;
                }



                String table_file_name = null;
                if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
                    table_file_name = "data/catalog/" + table_name + ".tbl";
                } else {
                    table_file_name = "data/user_data/" + table_name + ".tbl";
                }
//                out.println(table_name);
//                out.println(table_file_name);


//                out.println("where attr 1: "+whereAttribute_1);
//                out.println("where ops 1: "+whereOperator_1);
//                out.println("where values:");
//                for (int j = 0; j < whereValue_1.size(); j++) {
//                    out.println(whereValue_1.get(j));
//                }
//                if(whereConnectString !=null) {
//
//
//                    if (whereConnectString.equalsIgnoreCase("and") || whereConnectString.equalsIgnoreCase("or")) {
//                        out.println("where connect string: " + whereConnectString);
//                        out.println("where attr 2: " + whereAttribute_2);
//                        out.println("where ops 2: " + whereOperator_2);
//                        out.println("second where values:");
//                        for (int j = 0; j < whereValue_2.size(); j++) {
//                            out.println(whereValue_2.get(j));
//                        }
//                    }
//                }
//                out.println("reached");

                delete_records(table_name, table_file_name, whereClauseIncluded, whereAttribute_1,
                        whereOperator_1, whereValue_1.get(0), (whereClauseIncluded && (whereAttribute_1 != null) && whereAttribute_1.equals("rowid")),false);
//                out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
//                out.println("-----------------\n\n\n\n\n\n\n\n\n\n");
                out.println("Deletion Successful");
            }


        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }




    private static void delete_records(String table_name, String table_file_name,
                                       boolean whereClauseIncluded, String whereAttribute, String whereOperator,
                                       String whereValue, boolean rowidSearch,boolean deleteAll)
    {

        try {
//            out.println("entered read table");
//            out.println("table name: "+table_name );



            int root_page = getfromMetaTable(table_name, true, false);
//            out.println("root_page: "+root_page);
            if (root_page == -1) {
                System.out.println("Table '" + table_name + "' doesnt exist");
                return;
            }

            ArrayList<String> attribute_names = new ArrayList<String>();
            ArrayList<String> data_types = new ArrayList<String>();
            ArrayList<String> constraint_isNull = new ArrayList<String>();
            ArrayList<String> constraint_isUnique = new ArrayList<String>();
            ArrayList<String> constraint_isPK = new ArrayList<String>();

            if(!(table_name.equalsIgnoreCase("davisbase_tables") || table_name.equalsIgnoreCase("davisbase_columns")))
            {
//                out.println("adding row id to attribute names");
                attribute_names.add("rowid");
            }
            data_types.add("int");
            constraint_isNull.add("NO");
            constraint_isUnique.add("YES");
            constraint_isPK.add("YES");


            getColumnMetaData(table_name, attribute_names, data_types, constraint_isNull,constraint_isUnique,constraint_isPK);
//            out.println("return from getColumnMetaData");
//            out.println("the file table name: "+table_file_name );

            if(whereClauseIncluded) {
                if ((whereAttribute != null) && !attribute_names.contains(whereAttribute)) {
                    System.out.println("Column '" + whereAttribute + "' in 'where' condition is not present in table");
                    return;
                }
            }
//            out.println("attribute_names :" +attribute_names);
//            out.println("index of attribute : "+ whereAttribute +" in attribute_names is: "+attribute_names.indexOf(whereAttribute));

            int attrIndx=attribute_names.indexOf(whereAttribute)-1;

            RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "rw");

            int seek = 0;
            int rightSibling = 0;
            int startPage = 0;
            int records = 0;
            int endPage = Integer.MAX_VALUE - 1;
            int row_count = 0;

            if (rowidSearch) {
//                out.println("row id wise search");
                startPage = root_page;
                seek = (int) (startPage * pageSize);
                tableFile.seek(seek);
                int prevRowId,nextRowId,prevPageNo;

                while (tableFile.readByte() != 13) {
                    tableFile.seek(seek + 2);
                    records = tableFile.readShort();

                    int posToRead = seek + 16 + (2 * (records-1)); // last written cell ( top most)
                    tableFile.seek(posToRead);
                    posToRead = tableFile.readShort();
                    tableFile.seek(posToRead);
                    prevRowId =nextRowId=tableFile.readInt();
                    if (Integer.parseInt(whereValue) <= prevRowId) {
                        prevPageNo = tableFile.readInt();
                        while ((records > 1) && (Integer.parseInt(whereValue) < nextRowId)) {
                            records--;
                            posToRead = seek + 16 + (2 * (records-1));
                            tableFile.seek(posToRead);
                            posToRead = tableFile.readShort();
                            tableFile.seek(posToRead);
                            prevRowId = tableFile.readInt();
                            prevPageNo = tableFile.readInt();

                            if (records != 1) {
                                records--;
                                posToRead = seek + 16 + (2 * (records-1));
                                tableFile.seek(posToRead);
                                posToRead = tableFile.readShort();
                                tableFile.seek(posToRead);
                                nextRowId = tableFile.readInt();
                                records++;
                            }
                        }
                        startPage = prevPageNo;
                    } else {
                        tableFile.seek(seek + 6);
                        startPage = tableFile.readInt();
                    }
                    seek = (int) (startPage * pageSize);
                    tableFile.seek(seek);
                }
                if (whereOperator.contains("<")) {
                    endPage = startPage;
                    startPage = 0;
                }
                if (whereOperator.equals("=")) {
                    endPage = startPage ;
                }
            }

//            out.println("start page"+startPage);
//            out.println("end page"+endPage);
            boolean recordDeleted;
            while ((rightSibling != -1) && (rightSibling < (endPage + 1))) {
//                out.println("entered searching part");
                seek = (int) (startPage * pageSize);
//                out.println("seek value now: "+seek);
                //---------------------------
                tableFile.seek(seek + 6);
                rightSibling = tableFile.readInt();
                startPage = rightSibling;
                //---------------------------

                tableFile.seek(seek + 2);
                records = tableFile.readShort();
//                out.println("records value now: "+records);

                for (int i = 0; i < records; i++) {
                    recordDeleted=false;
//                    out.println("searching for records");
                    ArrayList<String> values = new ArrayList<String>();
                    int posToRead = seek + 16 + (2 * i);


                    tableFile.seek(posToRead);
                    if(deleteAll)
                    {
                        tableFile.writeShort(0);
                        continue;
                    }
                    posToRead = tableFile.readShort();
                    if(posToRead == 0)
                    {
//                        out.println("record deleted ");
                        continue;
                    }

//                    out.println("postoread value now: "+posToRead);

                    tableFile.seek(posToRead + 6);
                    int columns = tableFile.readByte();
//                    out.println("columns value now: "+columns);


                    tableFile.seek(posToRead + 2);

                    int rowidFound = tableFile.readInt();
                    if(whereAttribute.equalsIgnoreCase("rowid")) {
//                        out.println("rowid found " + rowidFound);
//                        out.println("where attribute" + Integer.parseInt(whereValue));
                        if (rowidFound == (Integer.parseInt(whereValue))) {
//                            out.println(" to be deleted record");
                            tableFile.seek(seek + 16 + (2 * i));
                            tableFile.writeShort(0);
                            continue;
                        }
                    }

                    int bytesRead = 0;

                    for (int k = 0; k <= attrIndx; k++) {
//                        out.println("for k= " + k);
//                        out.println("bytesRead till now :" + bytesRead);
                        tableFile.seek(posToRead + 6 + 1 + k);
                        int code = tableFile.readByte();
//                        out.println("code : " + code);
                        String type = getCodeDataType(code);
//                        out.println("type : " + type);
                        tableFile.seek(posToRead + 6 + 1 + columns + bytesRead);
                        String method;
                        String foundValue=null;

                        if(k==attrIndx) {
                            if (getDataTypeCode(type) == 0) {
                                method = "null";
                            } else {
                                method = getStoredDataType(type);
                            }
//                            out.println("method : " + method);

                            switch (method) {
                                case "null":
                                    foundValue = "null";
                                    break;
                                case "byte":
//                                    out.println("entered byte");
                                    foundValue = String.valueOf(tableFile.readByte());
                                    break;
                                case "short":
//                                    out.println("entered short");
                                    foundValue = String.valueOf(tableFile.readShort());
                                    break;
                                case "int":
                                case "integer":
//                                    out.println("entered integer");
                                    foundValue = String.valueOf(tableFile.readInt());
                                    break;
                                case "long":
//                                    out.println("entered long");
                                    foundValue = String.valueOf(tableFile.readLong());
                                    break;
                                case "float":
//                                    out.println("entered float");
                                    foundValue= String.valueOf(tableFile.readFloat());
                                    break;
                                case "double":
//                                    out.println("entered double");
                                    foundValue = String.valueOf(tableFile.readDouble());
                                    break;
                                default:
//                                    out.println("entered default");
                                    String temp = String.valueOf(tableFile.readLine());
                                    if (temp.equals("")) {
                                        foundValue = "null";
                                    } else {
                                        foundValue= temp;
                                    }
                                    break;
                            }
//                            out.println("found value :" +foundValue);
//                            out.println("whereAttribute value :" +whereValue);
                            if(foundValue.equals(whereValue)){
                                out.println("found record for deletion at " + (seek + 16 + (2 * i)));
                                recordDeleted=true;
                                out.println("deleted record");
                                tableFile.seek(seek + 16 + (2 * i));
                                out.println("read position "+ tableFile.readShort());
                                tableFile.seek(seek + 16 + (2 * i));
                                tableFile.writeShort(0);
                                break;
                            }
                        }
                        if (type.equals("text")) {
//                            out.println("entered here");
                            bytesRead = (bytesRead + (code - 12));

                        } else {
                            bytesRead = bytesRead + getDataTypeSize(type);
                        }


                    }
//                    out.println("reached out of loop");
                    if(recordDeleted)
                    {
                        continue;
                    }

                }

            }


        }
        catch (Exception e) {
            System.out.println(err_msg);
        }

    }

    public static void parseUpdate(String command) {
//        UPDATE, table_name, SET, column_name, =, value, WHERE ,attribute ,=,value;
        ArrayList<String> selectTokens = commandStringToTokenList(command);
//        out.println("selectTokens :" + selectTokens);

        try {
            String updtAttribute_1 = null;
            String updtValue_1 = null;
            String whereAttribute_1 = null;
            String whereOperator_1 = null;
//            String tempUpdtValue=null;
            ArrayList<String> whereValue_1 = new ArrayList<String>();
            int i=1;
            String table_name;
            table_name=selectTokens.get(i++);
//            out.println("table_name: "+ table_name);
            if(!selectTokens.get(i).equalsIgnoreCase("set"))
            {
                out.println("Syntax error, please check help to get correct syntax");
                return;
            }
//                        0      1          2     3           4    5     6       7        8   9
            //        UPDATE, table_name, SET, column_name, =, value, WHERE ,attribute ,=,value;

            i=3;
            updtAttribute_1=selectTokens.get(i);
//            out.println("updtAttribute_1: "+ updtAttribute_1);
            i=5;
            String[] tempArray = null;
//            String tempUpdtValue = selectTokens.get(i++);
//            if (tempUpdtValue.startsWith("'")) {
//                tempArray = tempUpdtValue.split("'");
//            }
//            updtValue_1=tempArray[1];
            updtValue_1=selectTokens.get(i++).replace("'","");
//            out.println("updtValue_1: "+ updtValue_1);
            if(!selectTokens.get(i).equalsIgnoreCase("where"))
            {
                out.println("Update can't work without where condition, please check help to get correct syntax");
                return;
            }
            i=7;
            whereAttribute_1=selectTokens.get(i++);
//            out.println("whereAttribute_1: "+ whereAttribute_1);
            whereOperator_1=selectTokens.get(i);
            if (whereOperator_1.equalsIgnoreCase("is") && selectTokens.get(i+1).equalsIgnoreCase("not") )
            {
                whereOperator_1="IS NOT";
                i+=2;
            }else
            {
                i++;
            }
//            out.println("whereOperator_1: "+ whereOperator_1);

            whereValue_1.add(selectTokens.get(i++));

//            out.println("whereValue_1: "+ whereValue_1);

            String table_file_name = null;
            if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
                table_file_name = "data/catalog/" + table_name + ".tbl";
            } else {
                table_file_name = "data/user_data/" + table_name + ".tbl";
            }
            boolean rowIdsearch=false;
            if(whereAttribute_1.equalsIgnoreCase("rowid")){
                rowIdsearch=true;
            }

            update_records(table_name, table_file_name,
                    updtAttribute_1, updtValue_1, whereAttribute_1, whereOperator_1,
                    whereValue_1.get(0),rowIdsearch);
    out.println("\n\n\n\n\n\n\n update successful");

        } catch (Exception e) {
            System.out.println(err_msg);
        }

    }


    private static void update_records(String table_name, String table_file_name,
                                       String updtAttribute,String updtValue,String whereAttribute, String whereOperator,
                                       String whereValue, boolean rowidSearch)
    {

        try {
//            out.println("entered read table");
//            out.println("table name: "+table_name );



            int root_page = getfromMetaTable(table_name, true, false);
//            out.println("root_page: "+root_page);
            if (root_page == -1) {
                System.out.println("Table '" + table_name + "' doesnt exist");
                return;
            }

            ArrayList<String> attribute_names = new ArrayList<String>();
            ArrayList<String> data_types = new ArrayList<String>();
            ArrayList<String> constraint_isNull = new ArrayList<String>();
            ArrayList<String> constraint_isUnique = new ArrayList<String>();
            ArrayList<String> constraint_isPK = new ArrayList<String>();

            if(!(table_name.equalsIgnoreCase("davisbase_tables") || table_name.equalsIgnoreCase("davisbase_columns")))
            {
//                out.println("adding row id to attribute names");
                attribute_names.add("rowid");
            }
            data_types.add("int");
            constraint_isNull.add("NO");
            constraint_isUnique.add("YES");
            constraint_isPK.add("YES");


            getColumnMetaData(table_name, attribute_names, data_types, constraint_isNull,constraint_isUnique,constraint_isPK);
//            out.println("return from getColumnMetaData");
//            out.println("the file table name: "+table_file_name );


                if (!(attribute_names.contains(whereAttribute)) || !(attribute_names.contains(updtAttribute))) {
                    System.out.println("Column name entered is not  present in table");
                    return;
                }

//            out.println("attribute_names :" +attribute_names);
//            out.println("index of attribute : "+ whereAttribute +" in attribute_names is: "+attribute_names.indexOf(whereAttribute));

            int attrIndx=attribute_names.indexOf(whereAttribute)-1;
            int updtAttrIndx=attribute_names.indexOf(updtAttribute)-1;

            RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "rw");

            int seek = 0;
            int rightSibling = 0;
            int startPage = 0;
            int records = 0;
            int endPage = Integer.MAX_VALUE - 1;
            int row_count = 0;

            if (rowidSearch) {
//                out.println("row id wise search");
                startPage = root_page;
                seek = (int) (startPage * pageSize);
                tableFile.seek(seek);
                int prevRowId,nextRowId,prevPageNo;

                while (tableFile.readByte() != 13) {
                    tableFile.seek(seek + 2);
                    records = tableFile.readShort();

                    int posToRead = seek + 16 + (2 * (records-1)); // last written cell ( top most)
                    tableFile.seek(posToRead);
                    posToRead = tableFile.readShort();
                    tableFile.seek(posToRead);
                    prevRowId =nextRowId=tableFile.readInt();
                    if (Integer.parseInt(whereValue) <= prevRowId) {
                        prevPageNo = tableFile.readInt();
                        while ((records > 1) && (Integer.parseInt(whereValue) < nextRowId)) {
                            records--;
                            posToRead = seek + 16 + (2 * (records-1));
                            tableFile.seek(posToRead);
                            posToRead = tableFile.readShort();
                            tableFile.seek(posToRead);
                            prevRowId = tableFile.readInt();
                            prevPageNo = tableFile.readInt();

                            if (records != 1) {
                                records--;
                                posToRead = seek + 16 + (2 * (records-1));
                                tableFile.seek(posToRead);
                                posToRead = tableFile.readShort();
                                tableFile.seek(posToRead);
                                nextRowId = tableFile.readInt();
                                records++;
                            }
                        }
                        startPage = prevPageNo;
                    } else {
                        tableFile.seek(seek + 6);
                        startPage = tableFile.readInt();
                    }
                    seek = (int) (startPage * pageSize);
                    tableFile.seek(seek);
                }
                if (whereOperator.contains("<")) {
                    endPage = startPage;
                    startPage = 0;
                }
                if (whereOperator.equals("=")) {
                    endPage = startPage ;
                }
            }

//            out.println("start page"+startPage);
//            out.println("end page"+endPage);

            while ((rightSibling != -1) && (rightSibling < (endPage + 1))) {
//                out.println("entered searching part");
                seek = (int) (startPage * pageSize);
//                out.println("seek value now: "+seek);
                //---------------------------
                tableFile.seek(seek + 6);
                rightSibling = tableFile.readInt();
                startPage = rightSibling;
                //---------------------------

                tableFile.seek(seek + 2);
                records = tableFile.readShort();
//                out.println("records value now: "+records);

                for (int i = 0; i < records; i++) {

//                    out.println("searching for records");
                    ArrayList<String> values = new ArrayList<String>();
                    int posToRead = seek + 16 + (2 * i);

                    tableFile.seek(posToRead);
                    posToRead = tableFile.readShort();
//                    out.println("postoread value now: "+posToRead);

                    tableFile.seek(posToRead + 6);
                    int columns = tableFile.readByte();
//                    out.println("columns value now: "+columns);


//                    tableFile.seek(posToRead + 2);
//
//                    int rowidFound = tableFile.readInt();
//                    if(whereAttribute.equalsIgnoreCase("rowid")) {
//                        out.println("rowid found " + rowidFound);
//                        out.println("where attribute" + Integer.parseInt(whereValue));
//                        if (rowidFound == (Integer.parseInt(whereValue))) {
//                            out.println(" to be deleted record");
////                            tableFile.seek(seek + 16 + (2 * i));
////                            tableFile.writeShort(0);
//                            continue;
//                        }
//                    }

                    int bytesRead = 0;
                    int bytesRead1 = 0;
                    boolean updateDone=false;

                    for (int k = 0; k <= attrIndx; k++) {

//                        out.println("for k= " + k);
//                        out.println("bytesRead till now :" + bytesRead);
                        tableFile.seek(posToRead + 6 + 1 + k);
                        int code = tableFile.readByte();
//                        out.println("code : " + code);
                        String type = getCodeDataType(code);
//                        out.println("type : " + type);
                        tableFile.seek(posToRead + 6 + 1 + columns + bytesRead);
                        String method;
                        String foundValue=null;

                        if(k==attrIndx) {
                            if (getDataTypeCode(type) == 0) {
                                method = "null";
                            } else {
                                method = getStoredDataType(type);
                            }
//                            out.println("method : " + method);

                            switch (method) {
                                case "null":
                                    foundValue = "null";
                                    break;
                                case "byte":
//                                    out.println("entered byte");
                                    foundValue = String.valueOf(tableFile.readByte());
                                    break;
                                case "short":
//                                    out.println("entered short");
                                    foundValue = String.valueOf(tableFile.readShort());
                                    break;
                                case "int":
                                case "integer":
//                                    out.println("entered integer");
                                    foundValue = String.valueOf(tableFile.readInt());
                                    break;
                                case "long":
//                                    out.println("entered long");
                                    foundValue = String.valueOf(tableFile.readLong());
                                    break;
                                case "float":
//                                    out.println("entered float");
                                    foundValue= String.valueOf(tableFile.readFloat());
                                    break;
                                case "double":
//                                    out.println("entered double");
                                    foundValue = String.valueOf(tableFile.readDouble());
                                    break;
                                default:
//                                    out.println("entered default");
                                    String temp = String.valueOf(tableFile.readLine());
                                    if (temp.equals("")) {
                                        foundValue = "null";
                                    } else {
                                        foundValue= temp;
                                    }
                                    break;
                            }
//                            out.println("found value :" +foundValue);
//                            out.println("whereAttribute value :" +whereValue);
                            if(foundValue.equals(whereValue)){

                                for (int l = 0; l <= updtAttrIndx; l++) {
//                                    out.println("for l= " + l);
//                                    out.println("bytesRead1 till now :" + bytesRead1);
                                    tableFile.seek(posToRead + 6 + 1 + l);
                                    int code1 = tableFile.readByte();
//                                    out.println("code1 : " + code1);
                                    String type1 = getCodeDataType(code1);
//                                    out.println("type1 : " + type1);
                                    tableFile.seek(posToRead + 6 + 1 + columns + bytesRead1);
                                    String method1;
                                    if(l==updtAttrIndx) {
                                        if (getDataTypeCode(type1) == 0) {
                                            method1 = "null";
                                        } else {
                                            method1 = getStoredDataType(type1);
                                        }
//                                        out.println("method1 : " + method1);

                                        switch (method1) {
                                            case "null":
                                                break;
                                            case "byte":
//                                                out.println("entered byte");
                                                tableFile.write(Byte.parseByte(updtValue));
                                                break;
                                            case "short":
//                                                out.println("entered short");
                                                tableFile.writeShort(Short.parseShort(updtValue));
                                                break;
                                            case "int":
                                            case "integer":
//                                                out.println("entered integer");
                                                tableFile.writeInt(Integer.parseInt(updtValue));
                                                break;
                                            case "long":
//                                                out.println("entered long");
                                                tableFile.writeLong(Long.parseLong(updtValue));
                                                if (code1==11 && !updtValue.equals("0")) {
                                                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(updtValue);
                                                    tableFile.writeLong((date.getTime()));
                                                } else if (code1==10 && !updtValue.equals("0")) {
                                                    Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(updtValue);
                                                    tableFile.writeLong((date.getTime()));
                                                } else {
                                                    tableFile.writeLong(Long.parseLong(updtValue));
                                                }
                                                break;
                                            case "float":
//                                                out.println("entered float");
                                                tableFile.writeFloat(Float.parseFloat(updtValue));
                                                break;
                                            case "double":
//                                                out.println("entered double");
                                                tableFile.writeDouble(Double.parseDouble(updtValue));
                                                break;
                                            default:
//                                                out.println("entered default");
                                                tableFile.writeBytes(updtValue);
                                                break;
                                        }

                                        break;
                                     }

                                    if (type1.equals("text")) {
//                                        out.println("entered here");
                                        bytesRead1 = (bytesRead1 + (code1 - 12));

                                    } else {
                                        bytesRead1 = bytesRead1 + getDataTypeSize(type1);
                                    }

                                }
                                updateDone=true;
                                break;
                            }
                        }
                        if (type.equals("text")) {
//                            out.println("entered here");
                            bytesRead = (bytesRead + (code - 12));

                        } else {
                            bytesRead = bytesRead + getDataTypeSize(type);
                        }
                        if(updateDone)
                        {
                            break;
                        }

                    }
//                    out.println("reached out of loop");
                    if(updateDone)
                    {
                        continue;
                    }

                }

            }

        }
        catch (Exception e) {
            System.out.println(err_msg);
        }

    }

    public static void displayVersion() {
        System.out.println("myDavis Version v1.0");
        System.out.println(copyright);
    }

    public static void parseUserCommand(String userCommand) {

//        out.println("userCommand is : "+ userCommand);
        ArrayList<String> commandTokens =commandStringToTokenList(userCommand);

        switch (commandTokens.get(0)) {
            case "select":
                parseQuery(userCommand+" ;");
                break;
            case "drop":
                droptable(commandTokens.get(2));
                break;
            case "create":
                parseCreateTable(userCommand +";");
                break;
            case "delete":
                parseDelete(userCommand+";");
                break;
            case "update":
                parseUpdate(userCommand);
                break;
            case "insert":
                parseInsert(userCommand);
                break;
            case "help":
                help();
                break;
            case "show":
                if ((commandTokens.size() == 2) && commandTokens.get(1).equals("tables")) {
                    parseQuery("select * from davisbase_tables where isActive = 1 ;");
                } else {
                    System.out.println(err_msg);
                }
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                exitProgram = true;
                break;
            case "quit":
                exitProgram = true;
            default:
                System.out.println(err_msg);
                break;
        }
    }
    /**
     * Help: Display supported commands
     */
    public static void help() {
        System.out.println(line("*", 80));
        System.out.println("SUPPORTED COMMANDS\n");
        System.out.println("All commands below are case insensitive\n");
        System.out.println("SHOW TABLES;");
        System.out.println("\tDisplay the names of all tables.\n");
        System.out.println(
                "CREATE TABLE <table_name> ( <column_name> <data_type> [ NOT NULL ] [ UNIQUE ] [ PK ] );");
        System.out.println("INSERT INTO <table_name> VALUES ( value_list );");
        System.out.println("\tInserts a  record into a table. \n");
        System.out.println("SELECT <column_list> FROM <table_name> [WHERE <condition 1 > [AND/OR] <condition 2> ;");
        System.out.println("\tDisplay table records whose optional two conditions can be given; <condition > is <column_name> = <value>.\n");
        System.out.println("DROP TABLE <table_name>;");
        System.out.println("\t Removes 'Active' of the table from schema, removes offset references and removes the table file. This is a permanent deletion\n");
        System.out.println("DELETE FROM <table_name> WHERE <column_name> = <value> ;");
        System.out.println("\t Deletes record references in pages . WHERE condition MUST be provided. To delete entire table chose DROP Table option\n");
        System.out.println("UPDATE <table_name> SET <column_name> = <value> WHERE <condition> ;");
        System.out.println("\t Updates column with the given  value in the records matched by the  WHERE condition (MUST be provided).\n");
        System.out.println("VERSION;");
        System.out.println("\tDisplay the program version.\n");
        System.out.println("HELP;");
        System.out.println("\tDisplay this help information.\n");
        System.out.println("EXIT;");
        System.out.println("\tExit the program.\n");
        System.out.println(line("*", 80));
    }

    /**
     * Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(line("-", 80));
        System.out.println("Welcome to myDavisBase"); // Display the string.
        System.out.println("myDavisBase Version " + version);
        System.out.println(copyright);
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(line("-", 80));
    }

    public static void main(String[] args) {
        Map();
        File catalogDir = new File("/data/catalog");
        File userDir = new File("/data/user_data");
        if(!(catalogDir.exists() && userDir.exists()))
        {
            initializeDataStore();
        }
        splashScreen();

        String userCommand = "";

        while (!exitProgram) {
            System.out.print(prompt);
            /* toLowerCase() renders command case insensitive */
        userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
        System.out.println("Exiting...");

//        String query;


//            query = "CREATE TABLE dogs (dogID INT NOT NULL UNIQUE PK, dogName TEXT UNIQUE , dogBreed TEXT );";
//            parseCreateTable(query);
//
//            query = "insert into dogs values ( 1 , bheri, street )";
        //            query = "insert into dogs ( dogID, dogName,dogBreed)  values ( 1 , bheri, street )";
//            parseInsert(query);
//            query = "insert into dogs values ( 2 , choverman , home )";
//            parseInsert(query);
//            query = "insert into dogs values ( 3 , soaniel, police )";
//            parseInsert(query);
//            query = "insert into dogs values ( 4 , chonaan, street )";
//            parseInsert(query);
//
//        query="select * from davisbase_tables where isActive = 1 ;";

//            query="select * from dogs ; ";
//                query="update dogs set dogname = 'dama1' where dogid = 1 ;";
//            query="select * from davisbase_tables ; ";
//            query="select * from dogs where dogname='choverman' or dogbreed='street' ;";
//            parseQuery(query);
//        parseUpdate(query);

//            droptable("dogs");

//            query="DELETE FROM dogs WHERE dogName = 'choverman' ;";
//                    parseDelete(query.toLowerCase());myDavisBase
    }


    static int getPageSize() {
        return pageSize;
    }
}

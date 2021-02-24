package simpledb;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

public class SimpleDb {
    public static void main (String[] args)
            throws DbException, TransactionAbortedException {
        // convert a file
        switch (args[0]) {
            case "convert":
                try {
                    if (args.length < 3 || args.length > 5) {
                        System.err.println("Unexpected number of arguments to convert ");
                        return;
                    }
                    File sourceTxtFile = new File(args[1]);
                    File targetDatFile = new File(args[1].replaceAll(".txt", ".dat"));
                    int numOfAttributes = Integer.parseInt(args[2]);
                    Type[] ts = new Type[numOfAttributes];
                    char fieldSeparator = ',';

                    if (args.length == 3)
                        for (int i = 0; i < numOfAttributes; i++)
                            ts[i] = Type.INT_TYPE;
                    else {
                        String typeString = args[3];
                        String[] typeStringAr = typeString.split(",");
                        if (typeStringAr.length != numOfAttributes) {
                            System.err.println("The number of types does not agree with the number of columns");
                            return;
                        }
                        int index = 0;
                        for (String s : typeStringAr) {
                            if (s.equalsIgnoreCase("int"))
                                ts[index++] = Type.INT_TYPE;
                            else if (s.equalsIgnoreCase("string"))
                                ts[index++] = Type.STRING_TYPE;
                            else {
                                System.err.println("Unknown type " + s);
                                return;
                            }
                        }
                        if (args.length == 5)
                            fieldSeparator = args[4].charAt(0);
                    }

                    HeapFileEncoder.convert(sourceTxtFile, targetDatFile,
                            BufferPool.getPageSize(), numOfAttributes, ts, fieldSeparator);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "print":
                File tableFile = new File(args[1]);
                int columns = Integer.parseInt(args[2]);
                DbFile table = Utility.openHeapFile(columns, tableFile);
                TransactionId tid = new TransactionId();
                DbFileIterator it = table.iterator(tid);

                if (null == it) {
                    System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
                } else {
                    it.open();
                    while (it.hasNext()) {
                        Tuple t = it.next();
                        System.out.println(t);
                    }
                    it.close();
                }
                break;
            case "parser":
                // Strip the first argument and call the parser
                String[] newargs = new String[args.length - 1];
                System.arraycopy(args, 1, newargs, 0, args.length - 1);

                try {
                    //dynamically load Parser -- if it doesn't exist, print error message
                    Class<?> c = Class.forName("simpledb.Parser");
                    Class<?> s = String[].class;

                    java.lang.reflect.Method m = c.getMethod("main", s);
                    m.invoke(null, (Object) newargs);
                } catch (ClassNotFoundException cne) {
                    System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
                } catch (Exception e) {
                    System.out.println("Error in parser.");
                    e.printStackTrace();
                }

                break;
            default:
                System.err.println("Unknown command: " + args[0]);
                System.exit(1);
        }
    }

}

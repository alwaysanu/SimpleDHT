package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.os.AsyncTask;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int SERVER_PORT = 10000;
    private static final int TEST_CNT = 50;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private Uri mUri = null;
    private String myPort = "";
    ContentResolver mContentResolver;
    Map<String,String> port_Hash = new HashMap<String, String>();
    Map<String,String> genHashInput = new HashMap<String, String>();

    String predecessor = null;
    String successor = null;

    List<String> availablePorts = new ArrayList();

    public SimpleDhtProvider() {

    }


    public SimpleDhtProvider(ContentResolver _cr) {
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i("in delete"," in provider");
        Log.i("selection is",selection);
        try {
            if (selection.equalsIgnoreCase("@")) {
                File f = getContext().getFilesDir();
                File[] files = f.listFiles();
                Log.i("total file count delete", String.valueOf(files.length));
                for (int i = 0; i < files.length; i++) {
                    //files[i].getName();
                    Log.i("file get name-->", files[i].getName());
                    File file = new File(f, files[i].getName());
                    file.delete();
                    Log.i("file deleted-->", String.valueOf(i));
                }
            }
            else if (selection.equalsIgnoreCase("*")) {


                mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                Log.i("delete *", "calling @ first");
                int res = delete(mUri,"@",null);


                if(predecessor!= null && successor!= null) {
                    Log.i("came inside","predecessor and successor null check");

                    if(!myPort.equalsIgnoreCase(REMOTE_PORT0)) {

                        String msgToSend = "getPorts" + "_" + REMOTE_PORT0 + "_" + REMOTE_PORT0;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT0));
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        Log.i("before write utf", "in query select all");
                        Log.i("msg to send", msgToSend);
                        os.writeUTF(msgToSend);
                        Log.i("before flush", "in query select All");
                        os.flush();
                        Log.i("after flush", "in query select All");


                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        Log.i("before read utf", "in query");
                        String serverMessage = is.readUTF();
                        Log.i("after read utf", "in query");
                        is.close();


                        String[] livePorts = serverMessage.split(":");
                        for(String p: livePorts){
                            Log.i("recieved available port",p);
                            availablePorts.add(p);
                        }
                        socket.close();



                    }
                    else {
                        Log.i("avd0 getting available","ports from itself");

                        for(Map.Entry<String,String> entry: port_Hash.entrySet()){
                            availablePorts.add(entry.getKey());
                        }
                        Log.i("No of available ports1",String.valueOf(availablePorts.size()));
                    }

                    Log.i("No of available ports2",String.valueOf(availablePorts.size()));
                    for(String s: availablePorts){
                        Log.i("live port-->",s);
                    }

                    Log.i("before iterating","on available ports");
                    Log.i("my port is:", myPort);
                    for (String port : availablePorts) {
                        Log.i("my port in loop",myPort);
                        if (port != myPort) {
                            Log.i("port in loop", port);
                            String msgToSend = "delete" + "_" + port + "_" + "@";
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port));
                            DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                            Log.i("before write utf", "in delete select all");
                            Log.i("msg to send", msgToSend);
                            os.writeUTF(msgToSend);
                            Log.i("before flush", "in delete select All");
                            os.flush();
                            Log.i("after flush", "in delete select All");


                            DataInputStream is = new DataInputStream(socket.getInputStream());
                            Log.i("before read utf", "in delete");
                            String serverMessage = is.readUTF();
                            Log.i("after read utf", "in delete");
                            is.close();
                            socket.close();
                            if (!serverMessage.isEmpty() && serverMessage != null && serverMessage != " "){
                                Log.i("server msg", serverMessage);

                            }

                            Log.i("pinging next avd","in delete all");
                        }

                    }
                }

            }
            else {
                String genfileName = genHash(selection);
                Log.i("genHash name in delete",genfileName);
                    if(predecessor == null && successor == null){
                        Log.i("delete file", "both null");
                        Log.i("deleting--->",selection);
                        File dir = getContext().getFilesDir();
                        File file = new File(dir, selection);
                        file.delete();
                    }
                    else if((genHash(genHashInput.get(predecessor)).compareTo(genHash(genHashInput.get(myPort))) > 0 && genHash(genHashInput.get(successor)).compareTo(genHash(genHashInput.get(myPort))) > 0)
                            && ((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0 || genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0))){

                        Log.i("delete in min port", myPort);
                        Log.i("deleting--->",selection);
                        File dir = getContext().getFilesDir();
                        File file = new File(dir, selection);
                        file.delete();
                    }
                    else if((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0  && genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0)){
                        Log.i("delete proper condition", myPort);
                        Log.i("deleting--->",selection);
                        File dir = getContext().getFilesDir();
                        File file = new File(dir, selection);
                        file.delete();
                    }
                    else {

                        Log.i("delete call successor", successor);

                        String msgToSend = "delete" + "_" + successor + "_" + selection;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successor));
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        Log.i("before write utf", "in client");
                        Log.i("msg to send", msgToSend);
                        os.writeUTF(msgToSend);
                        Log.i("before flush", "in query");
                        os.flush();
                        Log.i("after flush", "in query");


                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        Log.i("before read utf", "in query");
                        String serverMessage = is.readUTF();
                        Log.i("after read utf", "in query");
                        Log.i("server msg", serverMessage);
                        is.close();
                        socket.close();

                    }

            }
        }
        catch (NoSuchAlgorithmException nsa) {
            nsa.printStackTrace();
        }
        catch(UnknownHostException uh){
            uh.printStackTrace();
        }
        catch(IOException io){
            io.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values){
        String fileName = (values.getAsString(KEY_FIELD));
        String fileContents = values.getAsString(VALUE_FIELD);
        Log.i("inside provider",fileContents);
        Log.i("inside provider key",fileName);
        DataOutputStream outputStream;
        String genfileName;

        try {
            Log.i("inside try", "before exception");
            genfileName = genHash(fileName);
            Log.i("provider genhashed name",genfileName);

            if(predecessor!=null)
            Log.i("insert predecessor",predecessor);
            if(successor!=null)
            Log.i("insert successor",successor);
            Log.i("my Port",myPort);

            if(predecessor!=null) {
                Log.i("pred - file", String.valueOf(genHash(genHashInput.get(predecessor)).compareTo(genfileName)));
                Log.i("port - file", String.valueOf(genHash(genHashInput.get(myPort)).compareTo(genfileName)));
                Log.i("pred - port", String.valueOf(genHash(genHashInput.get(predecessor)).compareTo(genHash(genHashInput.get(myPort)))));
            }


           if(predecessor == null && successor == null){
               Log.i("insert file", "both null");
            outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
            outputStream.writeUTF(fileContents);
            outputStream.close();
           }
           else if((genHash(genHashInput.get(predecessor)).compareTo(genHash(genHashInput.get(myPort))) > 0 && genHash(genHashInput.get(successor)).compareTo(genHash(genHashInput.get(myPort))) > 0)
            && ((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0 || genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0))){

                   Log.i("insert in min port", myPort);
                   outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
                   outputStream.writeUTF(fileContents);
                   outputStream.close();
               }
           else if((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0  && genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0)){
               Log.i("insert proper condition", myPort);
               outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
               outputStream.writeUTF(fileContents);
               outputStream.close();
           }
           else {

                Log.i("insert call successor",successor);
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor,fileName, fileContents);
               insertToSuccessor(successor,fileName,fileContents);
            }
        }
        catch (NoSuchAlgorithmException nsa) {
            nsa.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();

        }
        return uri;

    }

    public void insertToSuccessor(String successor, String fileName, String fileContents){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor,fileName, fileContents);

    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i("Myport---->",myPort);

        genHashInput.put("11108","5554");
        genHashInput.put("11112","5556");
        genHashInput.put("11116","5558");
        genHashInput.put("11120","5560");
        genHashInput.put("11124","5562");

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if(myPort.equalsIgnoreCase(REMOTE_PORT0)){
                port_Hash.put(myPort,genHash(genHashInput.get(myPort)));

            }

            else {
                Log.i("On create--->",myPort);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  "newNode", myPort,myPort, myPort);
            }



        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }




        return false;
    }

    public void queryAvd0(String currPort, String ReqPort){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  "selectAll", ReqPort,ReqPort,ReqPort,ReqPort);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Log.i("inside query", "before querying");

        String[] matrixColumns = {KEY_FIELD,VALUE_FIELD};
        MatrixCursor mc= new MatrixCursor(matrixColumns);
        String[] mRow = new String[2];

        try {
            Log.e("Selection key",selection);
            String genfileName = genHash(selection);
            if(selection.equalsIgnoreCase("*")) {
                mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                Log.i("query *", "calling @ first");
                Cursor res = query(mUri,null,"@",null,null);

                Log.i("cursor object rows",String.valueOf(res.getCount()));
               String[] names = res.getColumnNames();

               for(String name: names){
                   Log.i("array",name);
               }

                if (res.moveToFirst()){
                    do{
                        mRow[0] = res.getString(res.getColumnIndex("key"));
                        mRow[1] = res.getString(res.getColumnIndex("value"));
                        Log.i("key in cursor", String.valueOf(mRow[0]));
                        Log.i("value in cursor", String.valueOf(mRow[1]));
                        mc.addRow(mRow);

                    }while(res.moveToNext());
                }
                if(predecessor!= null && successor!= null) {
                    Log.i("came inside","predecessor and successor null check");

                    if(!myPort.equalsIgnoreCase(REMOTE_PORT0)) {
                        String msgToSend = "getPorts" + "_" + REMOTE_PORT0 + "_" + REMOTE_PORT0;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT0));
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        Log.i("before write utf", "in query select all");
                        Log.i("msg to send", msgToSend);
                        os.writeUTF(msgToSend);
                        Log.i("before flush", "in query select All");
                        os.flush();
                        Log.i("after flush", "in query select All");


                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        Log.i("before read utf", "in query");
                        String serverMessage = is.readUTF();
                        Log.i("after read utf", "in query");
                        is.close();


                         String[] livePorts = serverMessage.split(":");
                        for(String p: livePorts){
                            Log.i("recieved available port",p);
                            availablePorts.add(p);
                        }
                        socket.close();



                    }
                    else {
                        Log.i("avd0 getting available","ports from itself");

                        for(Map.Entry<String,String> entry: port_Hash.entrySet()){
                            availablePorts.add(entry.getKey());
                        }
                        Log.i("No of available ports1",String.valueOf(availablePorts.size()));
                    }

                    Log.i("No of available ports2",String.valueOf(availablePorts.size()));
                    for(String s: availablePorts){
                        Log.i("live port-->",s);
                    }

                    Log.i("before iterating","on available ports");
                    Log.i("my port is:", myPort);
                    for (String port : availablePorts) {
                        Log.i("my port in loop",myPort);
                        if (port != myPort) {
                            Log.i("port in loop", port);
                            String msgToSend = "selectAll" + "_" + port + "_" + port;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port));
                            DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                            Log.i("before write utf", "in query select all");
                            Log.i("msg to send", msgToSend);
                            os.writeUTF(msgToSend);
                            Log.i("before flush", "in query select All");
                            os.flush();
                            Log.i("after flush", "in query select All");


                            DataInputStream is = new DataInputStream(socket.getInputStream());
                            Log.i("before read utf", "in query");
                            String serverMessage = is.readUTF();
                            Log.i("after read utf", "in query");
                            is.close();
                            socket.close();
                            if (!serverMessage.isEmpty() && serverMessage != null && serverMessage != " "){
                                Log.i("server msg", serverMessage);
                                String[] mcs = serverMessage.split("!");
                                for (String m : mcs) {
                                    Log.i("server response", m);
                                    String[] kv = m.split("%");
                                    for (String k : kv) {
                                        Log.i("kv --> ", k);
                                    }
                                    mRow[0] = kv[0];
                                    mRow[1] = kv[1];
                                    Log.i("key in selectAll", mRow[0]);
                                    Log.i("value in selectAll", mRow[1]);
                                    mc.addRow(mRow);
                                }

                            }

                            Log.i("pinging next avd","in query all");
                        }

                    }
                }

            }
            else if(selection.equalsIgnoreCase("@")){

                File f = getContext().getFilesDir();
                File[] files = f.listFiles();
                Log.i("total file count select",String.valueOf(files.length));
                for(int i=0; i<files.length;i++) {
                    DataInputStream inputStream;
                    Log.i("@ Selection", files[i].getName());
                    inputStream = new DataInputStream(getContext().openFileInput(files[i].getName()));
                    mRow[0] = files[i].getName();
                    mRow[1] = inputStream.readUTF();
                    Log.i("msg in query", String.valueOf(mRow[1]));
                    mc.addRow(mRow);

                    inputStream.close();
                }

            }
            else{

                if(predecessor == null && successor == null){
                    Log.i("select file", "both null");
                    DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
                    mRow[0] = selection;
                    mRow[1] = inputStream.readUTF();
                    Log.i("msg in query", String.valueOf(mRow[1]));
                    mc.addRow(mRow);
                    inputStream.close();
                }
                else if((genHash(genHashInput.get(predecessor)).compareTo(genHash(genHashInput.get(myPort))) > 0 && genHash(genHashInput.get(successor)).compareTo(genHash(genHashInput.get(myPort))) > 0)
                        && ((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0 || genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0))){

                    Log.i("select in min port", myPort);
                    DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
                    mRow[0] = selection;
                    mRow[1] = inputStream.readUTF();
                    Log.i("msg in query", String.valueOf(mRow[1]));
                    mc.addRow(mRow);
                    inputStream.close();
                }
                else if((genHash(genHashInput.get(predecessor)).compareTo(genfileName) <0  && genHash(genHashInput.get(myPort)).compareTo(genfileName) >= 0)){
                    Log.i("select proper condition", myPort);
                    DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
                    mRow[0] = selection;
                    mRow[1] = inputStream.readUTF();
                    Log.i("msg in query", String.valueOf(mRow[1]));
                    mc.addRow(mRow);
                    inputStream.close();
                }
                else {

                    Log.i("select call successor",successor);

                    String msgToSend = "querySucc"+"_"+successor+"_"+selection;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                    Log.i("before write utf","in client");
                    Log.i("msg to send",msgToSend);
                    os.writeUTF(msgToSend);
                    Log.i("before flush","in query");
                    os.flush();
                    Log.i("after flush","in query");


                    DataInputStream is = new DataInputStream(socket.getInputStream());
                    Log.i("before read utf","in query");
                    String serverMessage = is.readUTF();
                    Log.i("after read utf","in query");
                    Log.i("server msg",serverMessage);
                    is.close();


                    String[] mcs = serverMessage.split("#");

                    mRow[0] = mcs[0];
                    mRow[1] = mcs[1];

                    Log.i("key in query 4m succ", mRow[0]);
                    Log.i("value in query 4m succ", mRow[1]);
                    mc.addRow(mRow);
                    socket.close();

                }

            }
            Log.e(TAG, "Inside query");
        }
        catch(UnknownHostException uh){
            uh.printStackTrace();
        }
        catch(IOException io){
            io.printStackTrace();
        }
        catch (Exception e) {
            Log.e(TAG, e.toString());
        }




        Log.v("query", selection);

        Log.i("before returning ","matrix cursor");

        Log.i("mc no of rows",String.valueOf(mc.getCount()));


        return mc;
    }

    void callClient(String operation, String port, String predecessor, String successor){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  operation, port,predecessor,successor);
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String clientMessage;
            String seq = "";
            Socket s = null;
            Log.e("At server", myPort);
            while (true) {

                try {
                    Log.i("before accept", "in server");
                    s = serverSocket.accept();
                    Log.i("server--->", "accepted");
                    DataInputStream is = new DataInputStream(s.getInputStream());
                    Log.e("At server" + myPort, " Read the client message");
                    if ((clientMessage = is.readUTF()) != null) {
                        Log.i("client message-->",clientMessage);
                        String[] portMsgSeq = clientMessage.split("_");

                        String msg = portMsgSeq[0];
                        String port = portMsgSeq[1];

                        Log.i("msg - port", msg+port);
                        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        if (msg.equalsIgnoreCase("newNode")) {
                            port_Hash.put(port, genHash(genHashInput.get(port)));

                            Log.i("before sorting", "new node join");
                            for (Map.Entry<String, String> entry : port_Hash.entrySet()) {
                                Log.i("before sort", entry.getKey() + ":" + entry.getValue());
                            }

                            port_Hash = getSortedMap(port_Hash);
                            for (Map.Entry<String, String> entry : port_Hash.entrySet()) {
                                Log.i("after sort", entry.getKey() + ":" + entry.getValue());
                            }

                            if (port_Hash.size() == 1) {

                                for (String key : port_Hash.keySet()) {
                                    predecessor = key;
                                    successor = key;
                                }
                            } else {
                                String[] keys = new String[port_Hash.size()];


                                int i = 0;
                                for (String st : port_Hash.keySet()) {
                                    keys[i++] = st;
                                    Log.i("keys",st);
                                }

                                String response = " ";
                                String pred = " ";
                                String succ = " ";
                                for (int k = 0; k < port_Hash.size(); k++) {
                                    if (keys[k].equalsIgnoreCase(port)) {
                                        if (k == 0) {
                                            pred = keys[keys.length - 1];
                                            succ = keys[k + 1];
                                        } else if (k == keys.length - 1) {
                                            pred = keys[k - 1];
                                            succ = keys[0];
                                        }
                                        else {
                                            pred = keys[k - 1];
                                            succ = keys[k+1];
                                        }

                                    }
                                    if (keys[k].equalsIgnoreCase(REMOTE_PORT0)) {
                                        if (k == 0) {
                                            predecessor = keys[keys.length - 1];
                                            successor = keys[k + 1];
                                            Log.i("avd0 predecessor", predecessor);
                                            Log.i("avd0 predecessor", predecessor);
                                        } else if (k == (keys.length-1)) {
                                            predecessor = keys[k - 1];
                                            successor = keys[0];
                                            Log.i("avd0 predecessor", predecessor);
                                            Log.i("avd0 predecessor", predecessor);
                                        }
                                        else {
                                            predecessor = keys[k - 1];
                                            successor = keys[k+1];
                                            Log.i("avd0 predecessor", predecessor);
                                            Log.i("avd0 predecessor", predecessor);
                                        }
                                    }
                                }

                                response = pred + "_" + succ;
                                Log.i("response for node join",response);
                                DataOutputStream os = new DataOutputStream(s.getOutputStream());
                                Log.i("server sent", " acknowledgement");
                                os.writeUTF(response);
                                Log.i("Server sent ack", "acknow");
                            }

                        } else if (msg.equalsIgnoreCase("update")) {
                            Log.i("updating" + port, "successor,predecessor");
                            predecessor = (portMsgSeq[2]);
                            successor = (portMsgSeq[3]);
                            Log.i("updated successor->" + port, successor);
                            Log.i("updated predecessor->" + port, predecessor);
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("server sent", " acknowledgement");
                            os.writeUTF("updated");

                        } else if (msg.equalsIgnoreCase("updatepred")) {
                            Log.i("updating pre in ", myPort);
                            Log.i("as", port);
                            predecessor = port;
                            Log.i("predecessor now is",predecessor);
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("update pred in server", " sending ack");
                            os.writeUTF("pred updated");


                        } else if (msg.equalsIgnoreCase("updatesucc")) {
                            Log.i("updating succ in ", myPort);
                            Log.i("as", port);
                            successor = port;
                            Log.i("successor now is",successor);
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("update succ in server", " sending ack");
                            os.writeUTF("succ updated");
                        } else if (msg.equalsIgnoreCase("insert")) {
                            String fileName = portMsgSeq[2];
                            String fileContents = portMsgSeq[3];
                            Log.i("server insert",fileName);
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, fileName);
                            cv.put(VALUE_FIELD, fileContents);

                            insert(mUri, cv);
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("server called", " insert and sending ack");
                            os.writeUTF("insert called");

                        } else if (msg.equalsIgnoreCase("selectAll")) {
                            Cursor c = query(mUri, null, "@", null, null);

                            String[] names = c.getColumnNames();
                            List<String> l = new ArrayList();
                            if (c.moveToFirst()){
                                do{
                                    String name = c.getString(c.getColumnIndex("key"));
                                   String content = c.getString(c.getColumnIndex("value"));
                                    l.add(name+"%"+content);

                                }while(c.moveToNext());
                            }

                            StringBuilder res = new StringBuilder();
                            for(int i=0; i<l.size();i++){
                                if(i== l.size()-1)
                                    res.append(l.get(i));
                                else {
                                    res.append(l.get(i)).append("!");
                                }
                            }

                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("server msg selectAll",res.toString());
                            Log.i("select all called", " selecting and sending ack");
                            os.writeUTF(res.toString());


                        }
                        else if(msg.equalsIgnoreCase("getPorts")){
                            String[] keys = new String[port_Hash.size()];


                            int i = 0;
                            for (String st : port_Hash.keySet()) {
                                keys[i++] = st;
                                Log.i("keys to get all ports",st);
                            }
                            StringBuilder res = new StringBuilder();
                            for(int j=0; j<keys.length;j++){
                                if(j== keys.length-1)
                                    res.append(keys[j]);
                                else {
                                    res.append(keys[j]).append(":");
                                }
                            }


                            Log.i("sending all ports", res.toString());
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("avd0 called", " to get ports and sending ack");
                            os.writeUTF(res.toString());

                        }
                        else if(msg.equalsIgnoreCase("querySucc")){
                            String fileToSelect = portMsgSeq[2];
                            Cursor c = query(mUri, null, fileToSelect, null, null);
                            String[] names = c.getColumnNames();
                            String reply = " ";
                            //List<String> l = new ArrayList();
                            if (c.moveToFirst()){
                                do{
                                    String name = c.getString(c.getColumnIndex("key"));
                                    String content = c.getString(c.getColumnIndex("value"));
                                    Log.i("key querySucc", name);
                                    Log.i("value querySucc", content);
                                    //l.add(name+"$"+content);
                                    reply = name+"#"+content;

                                }while(c.moveToNext());
                            }

                            Log.i("send file succ server", reply);
                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("succ called", " to get file and sending ack");
                            os.writeUTF(reply);
                        }
                        else if(msg.equalsIgnoreCase("delete")){
                            String selector = portMsgSeq[2];
                            delete(mUri,selector,null);

                            DataOutputStream os = new DataOutputStream(s.getOutputStream());
                            Log.i("delete called", " server sending ack");
                            os.writeUTF("deleted");
                        }

                    }

                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "Can't create a ServerSocket");
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }

        }




            void callClient(String operation, String port, String predecessor, String successor){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  operation, port,predecessor,successor);
            }
            Map<String,String> getSortedMap(Map<String,String> map){

                List list = new ArrayList(map.entrySet());

                Collections.sort(list, new Comparator() {
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((Map.Entry) (o1)).getValue())
                                .compareTo(((Map.Entry) (o2)).getValue());
                    }
                });

                Map sortedHashMap = new LinkedHashMap();
                for (Iterator it = list.iterator(); it.hasNext();) {
                    Map.Entry entry = (Map.Entry) it.next();
                    sortedHashMap.put(entry.getKey(), entry.getValue());
                }
            return sortedHashMap;
            }
        }

    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            Log.i("msg0--->",msgs[0]);
            String operation = msgs[0];
            Log.i("msg1--->",msgs[1]);
            String portNum = msgs[1];
            String msgToSend = operation+"_"+portNum;
            if(operation.equalsIgnoreCase("update") || operation.equalsIgnoreCase("insert")){
                msgToSend = msgToSend+"_"+msgs[2]+"_"+msgs[3];
                Log.i("client update msg --->" + myPort,msgToSend);

            }else if(operation.equalsIgnoreCase("querySucc") || operation.equalsIgnoreCase("delete")){
                msgToSend = msgToSend+"_"+(msgs[2]);
            }

            String nodeJoinServer =  (operation.equalsIgnoreCase("newNode") || operation.equalsIgnoreCase("getPorts")) ? REMOTE_PORT0 : portNum;
            Log.i("node Join server",nodeJoinServer);
            String serverMessage;
            try{

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(nodeJoinServer));
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                Log.i("before write utf","in client");
                Log.i("msg to send",msgToSend);
                    os.writeUTF(msgToSend);
                    Log.i("before flush","in client");
                    os.flush();
                Log.i("after flush","in client");


                   DataInputStream is = new DataInputStream(socket.getInputStream());
                Log.i("before read utf","in client");
                    serverMessage = is.readUTF();
                Log.i("after read utf","in client");
                Log.i("server msg",serverMessage);

                if(serverMessage.contains("_")){

                    socket.close();
                   String [] ps = serverMessage.split("_");
                   predecessor = ps[0];
                   successor = ps[1];

                   Log.i("updated pred",predecessor);
                   Log.i("updated succ",successor);



                       Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                               Integer.parseInt(predecessor));
                       DataOutputStream os2 = new DataOutputStream(s.getOutputStream());

                       Log.i("calling pred-"+predecessor, "to update succ");
                       os2.writeUTF("updatesucc_" + myPort);
                       DataInputStream is2 = new DataInputStream(s.getInputStream());

                       serverMessage = is2.readUTF();
                       s.close();

                        Socket s2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successor));
                        Log.i("calling succ-"+successor, "to update pred");
                        DataOutputStream os3 = new DataOutputStream(s2.getOutputStream());
                        os3.writeUTF("updatepred_" + myPort);
                        DataInputStream is3 = new DataInputStream(s2.getInputStream());

                        serverMessage = is3.readUTF();
                        s2.close();

                }
                else if(serverMessage.contains(":")){


                    String[] livePorts = serverMessage.split(":");
                    for(String p: livePorts){
                        Log.i("recieved available port",p);
                        availablePorts.add(p);
                    }
                    socket.close();

                }
                else if(serverMessage.contains("$")){
                    socket.close();
                    String[] mcs = serverMessage.split("!");
                    for(String m: mcs){
                        String[] kv = m.split("$");
                    }
                }
                else if(serverMessage.contains("#")){
                    socket.close();
                    String[] mcs = serverMessage.split("#");

                }

                else
                socket.close();


                Log.i("client socket","recieved ack");
            }

            catch(UnknownHostException uh){
                uh.printStackTrace();
            }
            catch(IOException io){
                io.printStackTrace();
            }

            return null;
        }
    }
}

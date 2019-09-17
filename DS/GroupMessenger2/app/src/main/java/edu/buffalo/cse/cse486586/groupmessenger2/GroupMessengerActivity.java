package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.ArrayMap;
import android.util.Log;
import android.util.SparseArray;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;



/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int SERVER_PORT = 10000;
    private Uri mUri;
    private int count = 0;
    static final Map<String,Float> processNum = new HashMap<String, Float>();
    //Map<String,Integer> seqNum = new HashMap<String, Integer>();
    Map<String, List<Float>> msgMap = new HashMap<String, List<Float>>();
    PriorityQueue<Message>  pq = new PriorityQueue <Message>(150, new MessageComparator());
    int seqNum = 0;
    List<Float> propList = new ArrayList<Float>();
    private boolean isFailed = false;
    private String failedPort;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        processNum.put(REMOTE_PORT0,0.1f);
        processNum.put(REMOTE_PORT1,0.2f);
        processNum.put(REMOTE_PORT2,0.3f);
        processNum.put(REMOTE_PORT3,0.4f);
        processNum.put(REMOTE_PORT4,0.5f);


        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        final EditText et = (EditText) findViewById(R.id.editText1);

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        String msg = et.getText().toString() + "\n";
                        Log.i("-----on send click", msg);
                        et.setText("");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    }
                });

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String clientMessage;
            String seq = "";
            Socket s = null;
            try {
                while (true) {

                    try {
                        serverSocket.setSoTimeout(4000);
                        s = serverSocket.accept();


                        DataInputStream is = new DataInputStream(s.getInputStream());
                        if ((clientMessage = is.readUTF()) != null) {

                            String[] portMsgSeq = clientMessage.split("_");

                            String port = portMsgSeq[0];
                            //String senderSeq = portMsgSeq[1];

                            String msg = portMsgSeq[1];
                            Log.i("Server end message", msg);
                            boolean isupdated = (portMsgSeq[2].equalsIgnoreCase("T")) ? true : false;
                            String updatedSeq = portMsgSeq[3];
                            String senderPortNum = portMsgSeq[4];
                            failedPort = portMsgSeq[5];

                            Log.i("if Failed Port", String.valueOf(isFailed));
                            if (isFailed) {
                                Log.i("Failed Port", failedPort);
                                for (Message m : pq) {
                                    if (m.getPortNumber().equalsIgnoreCase(failedPort) && !m.isUpdated()) {
                                        pq.remove(m);
                                    }
                                }
                            }


                            Log.i("seq of this avd before", String.valueOf(seqNum));
                            if (!isupdated) {
                                seqNum += 1;
                                Log.i("seq of this avd inside", String.valueOf(seqNum));
                            }
                            Log.i("seq of this avd after", String.valueOf(seqNum));


                            float num = seqNum + processNum.get(port);
                            seq = String.valueOf(num);
                            Log.i("Server proposed seq-->", seq);
                            Log.i("server updated seq-->", updatedSeq);

                            if (isupdated && Float.valueOf(updatedSeq) > Float.valueOf(seq)) {
                                //seq = String.valueOf(Math.max(num,Float.valueOf(updatedSeq)));
                                float a = (Float.valueOf(updatedSeq));
                                seqNum = (int) a;
                            }

                            if (!isupdated) {
                                pq.add(new Message(msg, Float.parseFloat(seq), senderPortNum));
                            } else {
                                for (Message p : pq) {

                                    if (p.getMsg().equals(msg)) {
                                        pq.remove(p);
                                        Log.i("updating seq --->", msg);
                                        p.setSeqNo(Float.parseFloat(updatedSeq));
                                        p.setUpdated(true);
                                        pq.add(p);
                                    }
                                }
                            }
                            for (Message p : pq) {
                                Log.i("Queue", p.getMsg() + "--" + p.getSeqNo().toString() + "---" + "isupdated--" + p.isUpdated());
                            }

                            if (!pq.isEmpty()) {

                                while (pq.peek().isUpdated()) {
                                    Log.i("delivering--->", pq.peek().getMsg() + String.valueOf(pq.peek().getSeqNo()));
                                    publishProgress(pq.poll().getMsg());
                                    Log.e("server"," sfter publishing size of queue is: "+pq.size());
                                    if (pq.peek() == null)
                                        break;
                                }
                            }

                        }


                        DataOutputStream os = new DataOutputStream(s.getOutputStream());
                        os.writeUTF(seq);
                    }catch(SocketTimeoutException se){
                        se.printStackTrace();
                    while(!pq.isEmpty()){
                         Message m =  pq.poll();
                        if (m.isUpdated()) {
                            publishProgress(m.getMsg());
                            Log.e("server"," sfter publishing size of queue is: "+pq.size());
                        }
                    }
//                    Log.e(TAG, se.getMessage());
                } catch(StreamCorruptedException sce){
                        sce.printStackTrace();
                        while(!pq.isEmpty()){
                            Message m =  pq.poll();
                            if (m.isUpdated()) {
                                publishProgress(m.getMsg());
                                Log.e("server"," sfter publishing size of queue is: "+pq.size());
                            }
                        }
                    }
//                    Log.e(TAG, sce.getMessage());
                catch(EOFException eof){
                        eof.printStackTrace();
                    while(!pq.isEmpty()){
                        Message m =  pq.poll();
                        if (m.isUpdated()) {
                            publishProgress(m.getMsg());
                            Log.e("server"," sfter publishing size of queue is: "+pq.size());
                        }
                    }
//                        Log.e(TAG, "Eof exception");
                    }
                    catch(FileNotFoundException fnf){
                        fnf.printStackTrace();
                        while(!pq.isEmpty()){
                            Message m =  pq.poll();
                            if (m.isUpdated()) {
                                publishProgress(m.getMsg());
                                Log.e("server"," sfter publishing size of queue is: "+pq.size());
                            }
                        }
//                    Log.e(TAG, "File not Found");
                }  catch(IOException e){
                        e.printStackTrace();
                        while(!pq.isEmpty()){
                            Message m =  pq.poll();
                            if (m.isUpdated()) {
                                publishProgress(m.getMsg());
                                Log.e("server"," sfter publishing size of queue is: "+pq.size());
                            }
                        }
//                    Log.e(TAG, e.toString());
                }
            }
        } catch (Exception e) {
                e.printStackTrace();
                Log.e("client"," clientside exception");
            }

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            Log.i("in server on progress", strReceived);
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, Integer.toString(count));
            cv.put(VALUE_FIELD, strReceived);
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            getContentResolver().insert(mUri, cv);
            count++;
        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
        @Override
        protected Void doInBackground(String... msgs) {
            String remotePort = "";
            //try {
            Socket socket=null;
            String msgToSend = msgs[0];
            Log.i("recieved in client", msgToSend);
            String senderPortNo = msgs[1];
            try{
            for (String port : ports) {
                try {
                    //String senderSeq = String.valueOf(seqNum.get(senderPortNo));

                    remotePort = port;

                    Log.i("in client", "before connection");
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String finalMsg = port + "_" + msgToSend + "_" + "F" + "_" + "0" + "_" + senderPortNo + "_" + failedPort;
                    socket.setSoTimeout(2000);
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                    Log.i("client msg before flush", finalMsg);
                    os.writeUTF(finalMsg);
                    os.flush();

                    DataInputStream is = new DataInputStream(socket.getInputStream());
                    String proposedSeq = is.readUTF();
                    Log.i("proposed seq-->", proposedSeq);


                    /*if (msgMap.containsKey(senderPortNo + "_" + senderSeq)) {
                        msgMap.get(senderPortNo + "_" + senderSeq).add(Float.parseFloat(proposedSeq));
                    } else {*/

                    propList.add(Float.parseFloat(proposedSeq));
                    //Log.i("Add 2 msg map in client", senderPortNo + "_" + senderSeq+"->"+proposedSeq);
                    //msgMap.put(senderPortNo + "_" + senderSeq, l);
                    //}

                    socket.close();
                } catch (SocketTimeoutException se) {
                    se.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.i("in socketException", failedPort + isFailed);
                    socket.close();
                } catch (StreamCorruptedException sce) {
                    sce.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    socket.close();
                } catch (FileNotFoundException fnf) {
                    fnf.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.e(TAG, "File not Found");
                    socket.close();
                } catch (EOFException eof) {
                    eof.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.e(TAG, "Eof exception");
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.e(TAG, e.toString());
                    socket.close();
                }
            }


            for (String port : ports) {
                try {

                    remotePort = port;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String finalMsg = port + "_" + msgToSend + "_" + "T" + "_" + String.valueOf(Collections.max(propList)) + "_" + senderPortNo + "_" + failedPort;
                    Log.i("updated msg on client", finalMsg);
                    socket.setSoTimeout(2000);
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                    os.writeUTF(finalMsg);
                    os.flush();

                    DataInputStream is = new DataInputStream(socket.getInputStream());
                    String serverMessage = is.readUTF();

//                            if (serverMessage == "Y")
                    socket.close();
                } catch (SocketTimeoutException se) {
                    se.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.i("in socketException", failedPort + isFailed);
                    socket.close();
                } catch (StreamCorruptedException sce) {
                    sce.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    socket.close();
                } catch (FileNotFoundException fnf) {
                    fnf.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.e(TAG, "File not Found");
                    socket.close();
                } catch (EOFException eof) {
                    eof.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    Log.e(TAG, "Eof exception");
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    isFailed = true;
                    failedPort = remotePort;
                    socket.close();
                }
            }

        }catch (UnknownHostException e) {
                e.printStackTrace();
                Log.e("client"," UnknownHostException clientside exception");
            }catch(Exception e)
        {
            e.printStackTrace();
            Log.e(TAG, "exception");
        }



            return null;
        }
    }
}


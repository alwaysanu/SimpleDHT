package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "11108";
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static final int SERVER_PORT = 10000;
	private static final int TEST_CNT = 50;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private Uri mUri = null;
	private String myPort = "";
	ContentResolver mContentResolver;
	LinkedHashMap<String, String> port_Hash = new LinkedHashMap<String, String>();
	Map<String, String> genHashInput = new HashMap<String, String>();
	List<String> availablePorts = new ArrayList();
	Map<String, List<String>> succMap = new HashMap<String, List<String>>();
	Map<String, List<String>> predMap = new HashMap<String, List<String>>();
	String predecessor = null;
	String successor = null;
	String successor2 = null;
	String[] keys = {"11124", "11112", "11108", "11116", "11120"};
	boolean block = false;


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.i("in delete", " in provider");
		Log.i("selection is", selection);
		try {
			while(block) {
				Thread.sleep(1000);
			}
			if (selection.equalsIgnoreCase("@")) {
				File f = getContext().getFilesDir();
				File[] files = f.listFiles();
				Log.i("total_filecount_delete", String.valueOf(files.length));
				for (int i = 0; i < files.length; i++) {
					//files[i].getName();
					//Log.i("file get name-->", files[i].getName());
					File file = new File(f, files[i].getName());
					file.delete();
					//Log.i("file deleted-->", String.valueOf(i));
				}
			} else if (selection.equalsIgnoreCase("*")) {


				mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
				Log.i("delete *", "calling @ first");
				int res = delete(mUri, "@", null);


				Log.i("before iterating", "on available ports");
				Log.i("my port is:", myPort);
				for (String port : port_Hash.keySet()) {
					Log.i("my port in loop", myPort);
					if (!port.equalsIgnoreCase(myPort)) {
						Log.i("port in loop", port);
						String msgToSend = "delete" + "_" + port + "_" + "@";
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						DataOutputStream os = new DataOutputStream(socket.getOutputStream());
						os.writeUTF(msgToSend);
						os.flush();


						DataInputStream is = new DataInputStream(socket.getInputStream());
						String serverMessage = is.readUTF();
						is.close();
						socket.close();
						if (!serverMessage.isEmpty() && serverMessage != null && serverMessage != " ") {
							Log.i("server msg", serverMessage);

						}

						Log.i("pinging next avd", "in delete all");
					}

				}


			} else {

				String genfileName = genHash(selection);
				Log.i("genHash name in delete", genfileName);
				Log.i("delete in min port", myPort);
				Log.i("deleting--->", selection);
				File dir = getContext().getFilesDir();
				File file = new File(dir, selection);
				file.delete();

				//String m1 = "delete"+"_"+myPort+"_"+selection;
				String succ1 = succMap.get(myPort).get(0);
				String succ2 = succMap.get(myPort).get(1);
				String m2 = "justDelete"+"_"+succ1+"_"+selection;
				String m3 = "justDelete"+"_"+succ2+"_"+selection;
				callServer(m2,succ1);
				callServer(m3,succ2);

				String pred1 = predMap.get(myPort).get(0);
				String pred2 = predMap.get(myPort).get(1);
				String m4 = "justDelete"+"_"+pred1+"_"+selection;
				String m5 = "justDelete"+"_"+pred2+"_"+selection;
				callServer(m4,pred1);
				callServer(m5,pred2);

			}
		}
		catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		catch (SocketTimeoutException eof) {
			//eof.printStackTrace();
			return 0;
		}
		catch (EOFException eof) {
			//eof.printStackTrace();
			return 0;
		}
		catch (SocketException se) {
			//se.printStackTrace();
			return 0;
		}
		catch (NoSuchAlgorithmException nsa) {
			//nsa.printStackTrace();
			return 0;
		} catch (UnknownHostException uh) {
			//uh.printStackTrace();
			return 0;
		} catch (IOException io) {
			//io.printStackTrace();
			return 0;
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String fileName = (values.getAsString(KEY_FIELD));
		String fileContents = values.getAsString(VALUE_FIELD);
		Log.i("inside provider", fileContents);
		Log.i("inside provider key", fileName);
		DataOutputStream outputStream;

		try {

			Log.i("isBlocked in insert", String.valueOf(block));
			while(block) {
				Thread.sleep(1000);
			}
			Log.i("isBlocked after while", String.valueOf(block));
			Log.i("inside try", "before exception");
			String genfileName = genHash(fileName);
			Log.i("provider genhashed name", genfileName);

			int k = getPortToInsert(genfileName);
			String toPing = "";

			if (k == -1) {
				Log.i("inserting in", " min port");

				if(myPort.equalsIgnoreCase("11124")) {
					insertInSamePort(fileName,fileContents);
				}else{
					String msgToSend = "replicate" + "_" + "11124" + "_" + fileName + "_" + fileContents;
					callServer(msgToSend, "11124");
				}

				if(myPort.equalsIgnoreCase("11108")) {
					insertInSamePort(fileName,fileContents);
				}else {
					String msgToSend2 = "replicate" + "_" + "11108" + "_" + fileName + "_" + fileContents;
					callServer(msgToSend2, "11108");
				}

				if(myPort.equalsIgnoreCase("11112")) {
					insertInSamePort(fileName,fileContents);
				}else {
					String msgToSend3 = "replicate" + "_" + "11112" + "_" + fileName + "_" + fileContents;
					callServer(msgToSend3, "11112");
				}
			} else {
				toPing = keys[k];

				Log.i("checkCondition",String.valueOf(toPing.equalsIgnoreCase(myPort)));
				Log.i("to ping for insert", toPing);
				if (toPing.equalsIgnoreCase(myPort)) {
					Log.i("same port insert","first if in else");
					insertInSamePort(fileName,fileContents);
				}
				else{
					String msgToSend = "replicate" + "_" + toPing + "_" + fileName + "_" + fileContents;
					Log.i("same port insert","first if else in else");
					callServer(msgToSend, toPing);
				}

				String s1 = succMap.get(toPing).get(0);
				String s2 = succMap.get(toPing).get(1);
				Log.i("replicating in", s1 + "_" + s2);

				if(s1.equalsIgnoreCase(myPort)){
					Log.i("succ port insert","first if");
					insertInSamePort(fileName,fileContents);
				}
				else{
					String msgToSend2 = "replicate" + "_" + s1 + "_" + fileName + "_" + fileContents;
					Log.i("succ port insert","first if else");
					Log.i("msg-->",msgToSend2);
					callServer(msgToSend2, s1);
				}

				if(s2.equalsIgnoreCase(myPort)){
					insertInSamePort(fileName,fileContents);
				}
				else {
					String msgToSend3 = "replicate" + "_" + s2 + "_" + fileName + "_" + fileContents;
					callServer(msgToSend3, s2);
				}
			}

		} catch (NoSuchAlgorithmException nsa) {
			nsa.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();

		}

		return uri;

	}



	public void insertInSamePort(String fileName,String fileContents){

		DataOutputStream outputStream;
		try{
			File file = new File(getContext().getFilesDir(), fileName);
			Log.i("fileexits", String.valueOf(file.exists()));
			if (file.exists()) {
				Log.i("insidefileexits", "in same port replicate");
				DataInputStream inputStream = new DataInputStream(getContext().openFileInput(fileName));
				String value_version = inputStream.readUTF();
				Log.i("valueversionsameport", value_version);
				String[] vv = value_version.split(("_"));
				int version = Integer.parseInt(vv[1]);
				String value = vv[0];
				inputStream.close();


				outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
				String newMsg = fileContents + "_" + String.valueOf(version + 1);
				Log.i("newvalue&version", newMsg);
				outputStream.writeUTF(newMsg);
				outputStream.close();
			} else {
				outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
				outputStream.writeUTF(fileContents + "_" + "1");
				outputStream.close();
			}
		}
		catch(IOException io){
			io.printStackTrace();
		}

	}
	public int getPortToInsert(String genfileName) {
		int[] r = new int[5];

		int p = 0;
		for (Map.Entry<String, String> entry : port_Hash.entrySet()) {
			r[p] = (entry.getValue().compareTo(genfileName));
			p++;
		}

		int k = -1;
		for (int i = 0; i < r.length; i++) {
			if (i != r.length - 1 && r[i] < 0 && r[i + 1] >= 0) {
				k = i + 1;
				break;
			}
		}

		return k;
	}


	public boolean callServer(String msgToSend, String server) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(server));
			DataOutputStream os = new DataOutputStream(socket.getOutputStream());
			Log.i("calling--->" + server, "direct insert call");
			Log.i("msg to send", msgToSend);
			os.writeUTF(msgToSend);
			Log.i("before flush", "direct insert call");
			os.flush();
			Log.i("after flush", "direct insert call");


			DataInputStream is = new DataInputStream(socket.getInputStream());
			Log.i("response from " + server, "direct insert call");
			String serverMessage = is.readUTF();
			Log.i("response from " + server, serverMessage);
			is.close();
		}
		catch (SocketException se) {
			se.printStackTrace();
			return false;
		}catch (IOException io) {
			io.printStackTrace();
			return false;
		}
		return true;

	}


	public Map<String, KeyValuePair> callServerToGetAll(String msgToSend, String server) {
		String serverMessage = "";
		Map<String, KeyValuePair> res = new HashMap<String, KeyValuePair>();
		try {
			Log.i("getAllFrom", server);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(server));
			DataOutputStream os = new DataOutputStream(socket.getOutputStream());
			Log.i("query in " + server, "direct query call");
			Log.i("msg to send", msgToSend);
			os.writeUTF(msgToSend);
			Log.i("before flush", "direct query call");
			os.flush();
			Log.i("after flush", "direct query call");


			DataInputStream is = new DataInputStream(socket.getInputStream());
			Log.i("response from " + server, "direct query call");
			serverMessage = is.readUTF();
			Log.i("response from " + server, serverMessage);
			is.close();

			if(!serverMessage.isEmpty() &&  serverMessage != null) {
				String[] l = serverMessage.split("!");
				for (String s : l) {
					KeyValuePair k = new KeyValuePair();
					String[] kv = s.split("%");
					k.setKey(kv[0]);
					String[] vv = kv[1].split("_");
					k.setValue(vv[0]);
					k.setVersion(Integer.parseInt(vv[1]));

					res.put(k.getKey(), k);
				}
			}
		} catch (IOException io) {
			io.printStackTrace();
			return null;
		}
		return res;

	}

	public String justQuery(String msgToSend, String server){
		String serverMessage = "";
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(server));
			DataOutputStream os = new DataOutputStream(socket.getOutputStream());
			Log.i("query in " + server, "direct query call");
			Log.i("msg to send", msgToSend);
			os.writeUTF(msgToSend);
			Log.i("before flush", "direct query call");
			os.flush();
			Log.i("after flush", "direct query call");


			DataInputStream is = new DataInputStream(socket.getInputStream());
			Log.i("response from " + server, "direct query call");
			serverMessage = is.readUTF();
			Log.i("response from " + server, serverMessage);
			is.close();
		} catch (EOFException eof) {
			return null;
		} catch (SocketException se) {
			return null;
		} catch (IOException io) {
			return null;
		}
		return serverMessage;

	}

	public void insertToSuccessor(String successor, String fileName, String fileContents) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", successor, fileName, fileContents);

	}

	public void insertToSuccessorReplicate(String successor, String fileName, String fileContents) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replicate", successor, fileName, fileContents);

	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.i("Myport---->", myPort);
		try {
			port_Hash.put("11124", genHash("5562"));
			port_Hash.put("11112", genHash("5556"));
			port_Hash.put("11108", genHash("5554"));
			port_Hash.put("11116", genHash("5558"));
			port_Hash.put("11120", genHash("5560"));

			for (String k : port_Hash.keySet()) {
				Log.i("on create-->", k);
			}


			genHashInput.put("11108", "5554");
			genHashInput.put("11112", "5556");
			genHashInput.put("11116", "5558");
			genHashInput.put("11120", "5560");
			genHashInput.put("11124", "5562");

			succMap.put("11124", new ArrayList<String>(Arrays.asList("11112", "11108")));
			succMap.put("11112", new ArrayList<String>(Arrays.asList("11108", "11116")));
			succMap.put("11108", new ArrayList<String>(Arrays.asList("11116", "11120")));
			succMap.put("11116", new ArrayList<String>(Arrays.asList("11120", "11124")));
			succMap.put("11120", new ArrayList<String>(Arrays.asList("11124", "11112")));


			predMap.put("11124", new ArrayList<String>(Arrays.asList("11120", "11116")));
			predMap.put("11112", new ArrayList<String>(Arrays.asList("11124", "11120")));
			predMap.put("11108", new ArrayList<String>(Arrays.asList("11112", "11124")));
			predMap.put("11116", new ArrayList<String>(Arrays.asList("11108", "11112")));
			predMap.put("11120", new ArrayList<String>(Arrays.asList("11116", "11108")));

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			Log.i("before file read", "on create");

			File file = new File(getContext().getFilesDir(), "join");
			if (file.exists()) {
				mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
				delete(mUri,"@",null);
				DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput("join", Context.MODE_PRIVATE));
				outputStream.writeUTF("joined already");
				outputStream.close();
				block = true;
				Log.i("nodeRecovery_onCreate", String.valueOf(block));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recovered", myPort, myPort, myPort);

			} else {
				Log.i("on create", "inserting join file");
				DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput("join", Context.MODE_PRIVATE));
				outputStream.writeUTF("joined already");
				outputStream.close();
			}


		} catch (NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		} catch (IOException io) {
			io.printStackTrace();
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {


		Log.i("inside_query", "before querying");


		String[] matrixColumns = {KEY_FIELD, VALUE_FIELD};
		MatrixCursor mc = new MatrixCursor(matrixColumns);
		String[] mRow = new String[2];


		try {
			Log.i("isBlocked in query", String.valueOf(block));
			while(block){
				Thread.sleep(1500);
			}
			Log.i("isBlocked after while", String.valueOf(block));
			Log.i("locked query for-->", myPort);
			Log.e("Selection key", selection);
			String genfileName = genHash(selection);
			if (selection.equalsIgnoreCase("*")) {
				mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
				Log.i("query *", "calling @ first");
				Cursor res = query(mUri, null, "@", null, null);

				Log.i("cursor object rows", String.valueOf(res.getCount()));
				String[] names = res.getColumnNames();

				for (String name : names) {
					Log.i("array", name);
				}
				if (res.moveToFirst()) {
					do {
						if (!"join".equalsIgnoreCase(res.getString(res.getColumnIndex("key")))) {
							mRow[0] = res.getString(res.getColumnIndex("key"));
							String vv = res.getString(res.getColumnIndex("value"));
							mRow[1] = vv.split("_")[0];
							mc.addRow(mRow);
						}

					} while (res.moveToNext());
				}

				for (String port : port_Hash.keySet()) {
					Log.i("my port in loop", myPort);
					if (!port.equalsIgnoreCase(myPort)) {
						Log.i("port in loop", port);
						String msgToSend = "selectAll" + "_" + port + "_" + port;
						String serverMessage = queryAll(port, msgToSend);
						if (!serverMessage.isEmpty() && serverMessage != null && serverMessage != " ") {
							Log.i("server msg", serverMessage);
							String[] mcs = serverMessage.split("!");
							for (String m : mcs) {
								Log.i("server response", m);
								String[] kv = m.split("%");
								for (String k : kv) {
									Log.i("kv --> ", k);
								}
								if (!"join".equalsIgnoreCase(kv[0])) {
									mRow[0] = kv[0];
									//mRow[1] = kv[1];
									mRow[1] = kv[1].split("_")[0];
									mc.addRow(mRow);
								}
							}

						}

						Log.i("pinging next avd", "in query all");
					}

				}
			} else if (selection.equalsIgnoreCase("@")) {

				File f = getContext().getFilesDir();
				File[] files = f.listFiles();
				Log.i("total file count select", String.valueOf(files.length));
				for (int i = 0; i < files.length; i++) {
					DataInputStream inputStream;
					Log.i("@ Selection", files[i].getName());
					inputStream = new DataInputStream(getContext().openFileInput(files[i].getName()));
					if (!"join".equalsIgnoreCase(files[i].getName())) {
						mRow[0] = files[i].getName();
						String vv = inputStream.readUTF();
						mRow[1] = vv.split("_")[0];
						Log.i("msg in query", String.valueOf(mRow[1]));
						mc.addRow(mRow);
					}

					inputStream.close();
				}

			} else {

				//List<KeyValuePair> kvp = new ArrayList<KeyValuePair>();

				String m1 = "", m2 = "", m3 ="";
				int v1 = 0, v2 =0, v3 =0;
				int k = getPortToInsert(genfileName);
				String toPing = "";
				Log.i("kvalue-->", String.valueOf(k));
				if (k == -1) {
					Log.i("first condition", String.valueOf(myPort.equalsIgnoreCase("11124")));
					Log.i("second condition", String.valueOf(succMap.get("11124").contains(myPort)));
					//KeyValuePair kv1 = new KeyValuePair();
					if (myPort.equalsIgnoreCase("11124")) {
						Log.i("samePortQuery", "in query");
						DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
						mRow[0] = selection;
						String vv = inputStream.readUTF();
						m1 = vv.split("_")[0];
						v1 = Integer.parseInt(vv.split("_")[1]);
						Log.i("msg in query", m1);
						//mc.addRow(mRow);
						inputStream.close();
					} else {
						Log.i("querying in", " min port");
						String msgToSend = "query" + "_" + "11124" + "_" + selection;
						String serverMessage = justQuery(msgToSend, "11124");

						if(serverMessage!=null) {
							String[] mcs = serverMessage.split("#");

							mRow[0] = mcs[0];
							String vv = mcs[1];
							m1 = vv.split("_")[0];
							v1 = Integer.parseInt(vv.split("_")[1]);

							Log.i("key in query same port", mRow[0]);
							Log.i("value in same port", m1);
							//mc.addRow(mRow);
						}
					}


					Log.i("querying successors","in query k = -1 case");
					String s1 = succMap.get("11124").get(0);
					String s2 = succMap.get("11124").get(1);
					if(s1.equalsIgnoreCase(myPort)){

						Log.i("samePortQuery", "in query succ1");
						DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
						mRow[0] = selection;
						String vv = inputStream.readUTF();
						m2 = vv.split("_")[0];
						v2 = Integer.parseInt(vv.split("_")[1]);
						Log.i("msg in query", m2);
						//mc.addRow(mRow);
						inputStream.close();
					}
					else {
						String msgToSend = "query" + "_" + s1 + "_" + selection;
						Log.i("msg", msgToSend);
						String serverMessage = justQuery(msgToSend, s1);

						if (serverMessage != null) {
							String[] mcs = serverMessage.split("#");

							mRow[0] = mcs[0];
							String vv = mcs[1];
							m2 = vv.split("_")[0];
							v2 = Integer.parseInt(vv.split("_")[1]);

							Log.i("key in query same port", mRow[0]);
							Log.i("value in same port", m2);
							//mc.addRow(mRow);
						}

					}


					if(s2.equalsIgnoreCase(myPort)){

						Log.i("samePortQuery", "in query succ2");
						DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
						mRow[0] = selection;
						String vv = inputStream.readUTF();
						m3 = vv.split("_")[0];
						v3 = Integer.parseInt(vv.split("_")[1]);
						Log.i("msg in query", m3);
						//mc.addRow(mRow);
						inputStream.close();
					}
					else {
						String msgToSend2 = "query" + "_" + s2 + "_" + selection;
						Log.i("msg2", msgToSend2);
						String serverMessage2 = justQuery(msgToSend2, s2);

						if (serverMessage2 != null) {
							String[] mcs = serverMessage2.split("#");

							mRow[0] = mcs[0];
							String vv = mcs[1];
							m3 = vv.split("_")[0];
							v3 = Integer.parseInt(vv.split("_")[1]);

							Log.i("key in query same port", mRow[0]);
							Log.i("value in same port", m3);
							//mc.addRow(mRow);
						}
					}
					int latest = Math.max(v1, Math.max(v2, v3));
					Log.i("latest version is",String.valueOf(latest));
					if(latest == v1 && v1 !=0)
						mRow[1] = m1;
					else if( latest == v2 && v2 !=0)
						mRow[1] = m2;
					else if(v3 !=0)
						mRow[1] = m3;

					mc.addRow(mRow);


				} else {

					toPing = keys[k];
					Log.i("pinging-->",toPing);
					Log.i("firstCondition",String.valueOf(toPing.equalsIgnoreCase(myPort)));
					Log.i("secondCondition",String.valueOf(succMap.get(myPort).contains(toPing)));
					if (toPing.equalsIgnoreCase(myPort)) {
						Log.i("samePortQuery", "in query k = -1 else");
						DataInputStream inputStream = new DataInputStream(getContext().openFileInput(selection));
						mRow[0] = selection;
						String vv = inputStream.readUTF();
						m1 = vv.split("_")[0];
						v1 = Integer.parseInt(vv.split("_")[1]);
						//Log.i("msg in query", String.valueOf(mRow[1]));
						//mc.addRow(mRow);
						inputStream.close();

					} else {
						Log.i("anotherPortquery", "in query k = -1 else");
						String msgToSend = "query" + "_" + toPing + "_" + selection;
						String serverMessage = justQuery(msgToSend, toPing);

						if(serverMessage!=null) {
							String[] mcs = serverMessage.split("#");

							mRow[0] = mcs[0];
							String vv = mcs[1];
							m1 = vv.split("_")[0];
							v1 = Integer.parseInt(vv.split("_")[1]);
							Log.i("key in query diff port", mRow[0]);
						}
						//Log.i("value in diff port", mRow[1]);
						//mc.addRow(mRow);
					}

					Log.i("querying successors","in query "+toPing);
					String s1 = succMap.get(toPing).get(0);
					String s2 = succMap.get(toPing).get(1);
					String msgToSend = "query" + "_" + s1 + "_" + selection;
					Log.i("msg", msgToSend);
					String serverMessage = justQuery(msgToSend, s1);

					if(serverMessage!=null) {
						String[] mcs = serverMessage.split("#");

						mRow[0] = mcs[0];
						String vv = mcs[1];
						m2 = vv.split("_")[0];
						v2 = Integer.parseInt(vv.split("_")[1]);

						Log.i("key in query same port", mRow[0]);
						Log.i("value in same port", m2);
						//mc.addRow(mRow);
					}

					String msgToSend2 = "query" + "_" + s2 + "_" + selection;
					Log.i("msg2", msgToSend2);
					String serverMessage2 = justQuery(msgToSend2, s2);

					if(serverMessage2!=null) {
						String[] mcs = serverMessage2.split("#");

						mRow[0] = mcs[0];
						String vv = mcs[1];
						m3 = vv.split("_")[0];
						v3 = Integer.parseInt(vv.split("_")[1]);

						Log.i("key in query same port", mRow[0]);
						Log.i("value in same port", m3);
						//mc.addRow(mRow);
					}

					int latest = Math.max(v1, Math.max(v2, v3));
					Log.i("latest version is",String.valueOf(latest));
					if(latest == v1)
						mRow[1] = m1;
					else if( latest == v2)
						mRow[1] = m2;
					else
						mRow[1] = m3;

					mc.addRow(mRow);

				}
			}
		}  catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		catch (NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		return mc;
	}

	public String queryAll(String port, String msgToSend) {
		try {
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

			return serverMessage;
		} catch (EOFException eof) {
			return "";
		} catch (SocketException se) {
			return "";
		} catch (IOException se) {
			return "";
		}
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			String clientMessage;
			String seq = "";
			Socket s = null;
			Log.e("At_server", myPort);
			while (true) {

				try {
					Log.i("before_accept", "in server");
					s = serverSocket.accept();
					Log.i("server--->", "accepted");
					while(block){
						Thread.sleep(1000);
					}

					DataInputStream is = new DataInputStream(s.getInputStream());
					Log.e("At_server" + myPort, " Read the client message");
					if ((clientMessage = is.readUTF()) != null) {

						Log.i("client message-->", clientMessage);
						String[] portMsgSeq = clientMessage.split("_");

						String msg = portMsgSeq[0];
						String port = portMsgSeq[1];
						Log.i("msg-port", msg+port);

						mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
						if (msg.equalsIgnoreCase("insert")) {
							String fileName = portMsgSeq[2];
							String fileContents = portMsgSeq[3];
							Log.i("server insert",fileName);
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, fileName);
							cv.put(VALUE_FIELD, fileContents);

							insert(mUri, cv);
							/*DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("server called", " insert and sending ack");
							os.writeUTF("insert called");*/

						}else if(msg.equalsIgnoreCase("replicate")){
							Log.i("replicate_msg",clientMessage);
							Log.i("replicate", "in server");
							String fileName = portMsgSeq[2];
							String fileContents = portMsgSeq[3];
							Log.i("replicate_msg_filename",fileName);
							Log.i("replicate_msg_Content",fileContents);


							File file = new File(getContext().getFilesDir(),fileName);
							Log.i("fileexits",String.valueOf(file.exists()));
							/*File f = getContext().getFilesDir();
							List l = Arrays.asList(f.listFiles());*/

							//DataInputStream inputStream = new DataInputStream(getContext().openFileInput(fileName));
							if(file.exists()){
								Log.i("insidefileexits", "in replicate server");
								DataInputStream inputStream = new DataInputStream(getContext().openFileInput(fileName));
								String value_version = inputStream.readUTF();
								Log.i("valueversionreplicate", value_version);
								String [] vv = value_version.split(("_"));
								int version = Integer.parseInt(vv[1]);
								//String value = vv[0];
								inputStream.close();

								DataOutputStream outputStream1;
								outputStream1 = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
								String newMsg = fileContents+"_"+String.valueOf(version+1);
								Log.i("newvalue&version", newMsg);
								outputStream1.writeUTF(newMsg);
								outputStream1.close();


							}else {

								Log.i("elseoffileexits", "in replicate server");
								DataOutputStream outputStream;
								outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
								outputStream.writeUTF(fileContents+"_"+String.valueOf(1));
								outputStream.close();
							}

							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("replicatedin "+port, "sending ack");
							os.writeUTF("replicated");
						}else if(msg.equalsIgnoreCase("directInsert")){

							String list = portMsgSeq[2];

							String [] files = list.split(":");
							for(String f : files){
								String [] keyvv = f.split("!");
									String fileName = keyvv[0];
									String value = keyvv[1];
									String version = keyvv[2];
									DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
									outputStream.writeUTF(value+"_"+version);
									outputStream.close();
								}

							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("directInsert", " selecting and sending ack");
							os.writeUTF("inserted");
							}
							/*String fileContents = portMsgSeq[3];
							String version = portMsgSeq[4];

							DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
							outputStream.writeUTF(fileContents+"_"+version);
							outputStream.close();

							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							//Log.i("server msg selectAll",res.toString());
							Log.i("directInsert", " selecting and sending ack");
							os.writeUTF("inserted");*/

						else if (msg.equalsIgnoreCase("selectAll")) {
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


						}else if (msg.equalsIgnoreCase("recoverAll")) {
							//Cursor c = query(mUri, null, "@", null, null);


							String[] matrixColumns = {KEY_FIELD,VALUE_FIELD};
							MatrixCursor mc2= new MatrixCursor(matrixColumns);
							String[] mRow2 = new String[2];
							File f = getContext().getFilesDir();
							File[] files = f.listFiles();
							Log.i("total file count select", String.valueOf(files.length));
							for (int i = 0; i < files.length; i++) {
								DataInputStream inputStream;
								//Log.i("@ Selection", files[i].getName());
								inputStream = new DataInputStream(getContext().openFileInput(files[i].getName()));
								if(!"join".equalsIgnoreCase(files[i].getName())) {
									mRow2[0] = files[i].getName();
									String vv = inputStream.readUTF();
									mRow2[1] = vv;//.split("_")[0];
									//Log.i("msg in query", String.valueOf(mRow2[1]));
									mc2.addRow(mRow2);
								}

								inputStream.close();
							}


							String[] names = mc2.getColumnNames();
							List<String> l = new ArrayList();
							if (mc2.moveToFirst()){
								do{
									String name = mc2.getString(mc2.getColumnIndex("key"));
									String content = mc2.getString(mc2.getColumnIndex("value"));
									l.add(name+"%"+content);

								}while(mc2.moveToNext());
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
							Log.i("server msg recoverAll",res.toString());
							Log.i("recoverAll called", " selecting and sending ack");
							os.writeUTF(res.toString());


						}
						else if(msg.equalsIgnoreCase("query")){
							String fileToSelect = portMsgSeq[2];
							DataInputStream inputStream = new DataInputStream(getContext().openFileInput(fileToSelect));
							String name = fileToSelect;
							String content = inputStream.readUTF();
							Log.i("msg in query", String.valueOf(content));
							//mc.addRow(mRow);
							inputStream.close();

							Log.i("send file succ server", name+"#"+content);
							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("succ called", " to get file and sending ack");
							os.writeUTF(name+"#"+content);
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
						else if(msg.equalsIgnoreCase("JustDelete")){
							String selector = portMsgSeq[2];
							//String genfileName = genHash(selector);
							Log.i("genHash name in delete", selector);
							Log.i("delete in min port", myPort);
							Log.i("deleting--->", selector);
							File dir = getContext().getFilesDir();
							File file = new File(dir, selector);
							file.delete();
							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("delete called", " server sending ack");
							os.writeUTF("deleted");
						}else if(msg.equalsIgnoreCase("deleteReplica")){
							String selector = portMsgSeq[2];
							File dir = getContext().getFilesDir();
							File file = new File(dir, selector);
							file.delete();

							DataOutputStream os = new DataOutputStream(s.getOutputStream());
							Log.i("delete replica", " server sending ack");
							os.writeUTF("deleted");
						}
					}

				}
				catch (InterruptedException ie) {
					ie.printStackTrace();
				}
				catch (IOException io) {
					io.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {
			Log.i("msg0--->", msgs[0]);
			String operation = msgs[0];
			Log.i("msg1--->", msgs[1]);
			String portNum = msgs[1];
			String msgToSend = operation + "_" + portNum;

			String serverMessage;
			try {

				Log.i("inside if","on create");
				String succ1 = succMap.get(myPort).get(0);
				String succ2 = succMap.get(myPort).get(1);
				Log.i("succ1 in oncreate",succ1);
				Log.i("succ2 in oncreate",succ2);
				Log.i("rejoined------->","getting all msgs from successors");

				Map<String,KeyValuePair> m1 = callServerToGetAll("recoverAll"+"_"+myPort,succ1);

				Map<String,KeyValuePair> m2 =callServerToGetAll("recoverAll"+"_"+myPort, succ2);

				//Map<String,KeyValuePair> m3 =callServerToGetAll("recoverAll"+"_"+myPort , myPort);
				//check and add if failed avd has msgs.
				//Log.i("file count in joined",String.valueOf(m3.size()));
				Log.i("succ1 file count joined",String.valueOf(m1.size()));
				Log.i("succ2 file count joined",String.valueOf(m2.size()));

				Map<String,String> finalNodeMap = new HashMap<String, String>();

				for(Map.Entry<String,KeyValuePair> entry : m1.entrySet()) {
					String genfileName = genHash(entry.getValue().getKey());

					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if (keys[l].equalsIgnoreCase(myPort)) {
						if (m2.containsKey(entry.getKey())) {

							if (m2.get(entry.getKey()).getVersion() > entry.getValue().getVersion()) {
								finalNodeMap.put(entry.getKey(), m2.get(entry.getKey()).getValue() + "!" + m2.get(entry.getKey()).getVersion());

							} else {
								finalNodeMap.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
							}

							m2.remove(entry.getKey());
						} else {
							finalNodeMap.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
						}
					}
				}



				for(Map.Entry<String,KeyValuePair> entry : m2.entrySet()) {
					String genfileName = genHash(entry.getKey());
					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if(keys[l].equalsIgnoreCase(myPort)){
						finalNodeMap.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());

					}
				}


				directInsertSamePort(portNum,finalNodeMap);
				directInsertList(succ1,finalNodeMap);
				directInsertList(succ2,finalNodeMap);

				String pred1 = predMap.get(myPort).get(0);
				String pred2 = predMap.get(myPort).get(1);
				Log.i("pred1 in oncreate",pred1);
				Log.i("pred2 in oncreate",pred2);

				Map<String,KeyValuePair> p1 = callServerToGetAll("recoverAll"+"_"+myPort,pred1);

				//Map<String,KeyValuePair> p2 =callServerToGetAll("recoverAll"+"_"+myPort, pred2);
				Map<String,KeyValuePair> m12 = callServerToGetAll("recoverAll"+"_"+myPort,succ1);

				//Map<String,KeyValuePair> p3 =callServerToGetAll("recoverAll"+"_"+myPort , myPort);

				//Map<String,KeyValuePair> p3 =callServerToGetAll("recoverAll"+"_"+myPort , myPort);
				//check and add if failed avd has msgs.
				//Log.i("file count in joined",String.valueOf(p3.size()));
				Log.i("pred1 file count joined",String.valueOf(p1.size()));
				Log.i("succ1 file count joined",String.valueOf(m12.size()));

				Map<String,String> finalNodeMap2 = new HashMap<String, String>();

				for(Map.Entry<String,KeyValuePair> entry : p1.entrySet()) {
					String genfileName = genHash(entry.getValue().getKey());
					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if (keys[l].equalsIgnoreCase(pred1)) {
						if (m12.containsKey(entry.getKey())) {

							if (m12.get(entry.getKey()).getVersion() > entry.getValue().getVersion()) {
								finalNodeMap2.put(entry.getKey(), m12.get(entry.getKey()).getValue() + "!" + m12.get(entry.getKey()).getVersion());

							} else {
								finalNodeMap2.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
							}

							m12.remove(entry.getKey());
						} else {
							finalNodeMap2.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
						}
					}
				}



				for(Map.Entry<String,KeyValuePair> entry : m12.entrySet()) {
					String genfileName = genHash(entry.getKey());
					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if(keys[l].equalsIgnoreCase(pred1)){
						finalNodeMap2.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());

					}
				}

				directInsertSamePort(portNum,finalNodeMap2);
				directInsertList(pred1,finalNodeMap2);
				directInsertList(succ1,finalNodeMap2);


				Map<String,KeyValuePair> px1 = callServerToGetAll("recoverAll"+"_"+myPort,pred1);

				//Map<String,KeyValuePair> p2 =callServerToGetAll("recoverAll"+"_"+myPort, pred2);
				Map<String,KeyValuePair> px2 = callServerToGetAll("recoverAll"+"_"+myPort,pred2);

				//Map<String,KeyValuePair> px3 =callServerToGetAll("recoverAll"+"_"+myPort , myPort);

				Log.i("file count in joined",String.valueOf(px1.size()));
				Log.i("pred1 file count joined",String.valueOf(px2.size()));
				//Log.i("pred2 file count joined",String.valueOf(px3.size()));

				Map<String,String> finalNodeMap3 = new HashMap<String, String>();

				for(Map.Entry<String,KeyValuePair> entry : px2.entrySet()) {
					String genfileName = genHash(entry.getValue().getKey());

					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if (keys[l].equalsIgnoreCase(pred2)) {
						if (px1.containsKey(entry.getKey())) {

							if (px1.get(entry.getKey()).getVersion() > entry.getValue().getVersion()) {
								finalNodeMap3.put(entry.getKey(), px1.get(entry.getKey()).getValue() + "!" + px1.get(entry.getKey()).getVersion());

							} else {
								finalNodeMap3.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
							}

							px1.remove(entry.getKey());
						} else {
							finalNodeMap3.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());
						}
					}
				}



				for(Map.Entry<String,KeyValuePair> entry : px1.entrySet()) {
					String genfileName = genHash(entry.getKey());
					int k = getPortToInsert(genfileName);
					int l = k == -1 ? 0 : k;
					if(keys[l].equalsIgnoreCase(pred2)){
						finalNodeMap3.put(entry.getKey(), entry.getValue().getValue() + "!" + entry.getValue().getVersion());

					}
				}

				directInsertSamePort(portNum,finalNodeMap3);
				directInsertList(pred1,finalNodeMap3);
				directInsertList(pred2,finalNodeMap3);


				block = false;
				Log.i("isBlocked", String.valueOf(block));
				Log.i("nodeRecovered","end of updation");



			}catch(NoSuchAlgorithmException nsae){
				nsae.printStackTrace();
			}



				return null;
		}

	}

	public void directInsertList(String port, Map<String,String> m){

		if(m.size()> 0) {
			String msgToSend = "";
			for (Map.Entry<String, String> entry : m.entrySet()) {
				msgToSend = msgToSend + (entry.getKey() + "!" + entry.getValue());
				msgToSend = msgToSend + ":";
			}

			String msg2 = "directInsert" + "_" + port + "_" + msgToSend;
			Log.i("updating " + port, "direct Insert");
			callServer(msg2, port);

		}
	}

	public void directInsertSamePort(String port, Map<String,String> m){
		try{
			for(Map.Entry<String,String> entry : m.entrySet()){
				String fileName = entry.getKey();
				String[] vv = entry.getValue().split(("!"));
				String value = vv[0];
				String version = vv[1];
				DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput(fileName, Context.MODE_PRIVATE));
				outputStream.writeUTF(value+"_"+version);
				outputStream.close();

			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}


		private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
       // Log.i("inside genhash", formatter.toString());
        return formatter.toString();
    }
}

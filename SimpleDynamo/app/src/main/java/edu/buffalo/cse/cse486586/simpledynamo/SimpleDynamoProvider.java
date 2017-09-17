package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

// Reference: Context: https://developer.android.com/reference/android/content/Context.html#getApplicationInfo()
// Reference: Previous assignment PA2A and PA2B codes
// Reference: Android: http://stackoverflow.com/questions/3554722/how-to-delete-internal-storage-file-in-android
// Reference: Android: http://stackoverflow.com/questions/12310577/iterate-through-a-specified-directory-in-android
// Reference: Content Values - https://developer.android.com/reference/android/content/ContentProvider.html#insert(android.net.Uri,%20android.content.ContentValues)
// Reference: Some suggestions from Piazza posts!
// Reference: Socket Programming - https://developer.android.com/reference/java/net/Socket.html
// Reference: List - https://docs.oracle.com/javase/tutorial/collections/interfaces/list.html
// Reference: Code from Previous Assignment PA3 - SimpleDHT

public class SimpleDynamoProvider extends ContentProvider {
	String myID;
	int portnumber = 0;
	int first_succ = 0;
	int second_succ = 0;
	int first_pred = 0;
	int second_pred = 0;
	boolean stop = false;
	boolean firstsucc = false;
	boolean recoveryflag = false;
	int leaderport = 11108;
	boolean queryisreturned = false;
	String pred = null;
	String succ = null;
	HashMap<String,String> map_Hash = new HashMap<String, String>();
	HashMap<String,String> map_Succ = new HashMap<String, String>();
	HashMap<String,String> map_Pred = new HashMap<String, String>();
	List<String> hash_values = new ArrayList<String>();
	List<String> Nodes = new ArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		try {
				String filename = selection;
				String current_hash = genHash(filename);
				String node_Hash = null;
				for (int i = 0; i < Nodes.size(); i++) {
					String locHash = Nodes.get(i);


					if (locHash.compareTo(current_hash) >= 0) {
						node_Hash = locHash;
						break;

					} else {
						if (i == (Nodes.size() - 1)) {
							node_Hash = Nodes.get(0);
						}
					}

				}

				String avd = map_Hash.get(node_Hash);

				if(avd.equals(myID)){
					System.out.println("Delete:Key" + filename + ":TobeDeleted from " + myID);
					getContext().deleteFile(filename);
					String insert_msg="DeleteFrom";
					String avd_fname = first_succ+":"+filename;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname);
					String avd_sname = second_succ+":"+filename;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_sname);
				}
				else
				{
					String insert_msg="DeleteFrom";
					String avd_fname = avd+":"+filename;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname);
					String[] succ = map_Succ.get(avd).split(":");
					String succ1 = succ[0];
					String succ2 = succ[1];
					String avd_sname = succ1+":"+filename;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_sname);
					String avd_tname = succ2+":"+filename;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_tname);

				}




		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String BelongsTo(String key) throws NoSuchAlgorithmException {
		String node_Hash = null;
		String current_hash = key;
		//String current_hash = genHash(key);
//		System.out.println("Inside Belongs To");
		Collections.sort(Nodes);
//		System.out.println("Hash_Values List:" + Nodes);

		for (int i = 0; i < Nodes.size(); i++) {
			String locHash = Nodes.get(i);

			if (locHash.compareTo(current_hash) >= 0) {
				node_Hash = locHash;
				break;

			}
			else{
				if (i== (Nodes.size()-1)){
					node_Hash=Nodes.get(0);
				}
			}

		}

		String avd = map_Hash.get(node_Hash);
	return avd;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		try {
			while(recoveryflag){
				System.out.println("Waiting For Recovery @Insert");
				Thread.sleep(200);
			}
			String filename = (String) values.get("key");
			String val = (String) values.get("value");
			String avd = BelongsTo(genHash(filename));
			System.out.println("Key " + filename + " Val" + val + " Belongs to AVD: " + avd);
			if (avd.equals(myID)) {
				System.out.println("INSERT:Key " + filename+" val "+val + ":TobeStoredIn" + portnumber);
				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(filename, Context.MODE_PRIVATE));
				outputStreamWriter.write(val.toString());
				outputStreamWriter.close();
				String insert_msg = "InsertIntoSucc";
				String avd_fname_val = first_succ + ":" + filename + ":" + val;
				System.out.println("INSERT(Replicate):Key " + filename +" val "+ val +"Sending to succ "+first_succ);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname_val);
				avd_fname_val = second_succ + ":" + filename + ":" + val;
				System.out.println("INSERT(Replicate):Key " + filename +" val "+ val +"Sending to succ "+second_succ);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname_val);
			} else {
				//map_Succ.get(avd);
				String[] succ = map_Succ.get(avd).split(":");
				String succ1 = succ[0];
				String succ2 = succ[1];
				String insert_msg = "InsertIntoSucc";
				System.out.println("INSERT:Key " + filename +" val "+ val +":TobeStoredIn" + portnumber+"Sending to succ "+succ);
				String avd_fname_val = avd + ":" + filename + ":" + val;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname_val);
				avd_fname_val = succ1 + ":" + filename + ":" + val;
				System.out.println("INSERT(Replicate):Key " + filename +" val "+ val +"Sending to succ "+succ1);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname_val);
				System.out.println("INSERT(Replicate):Key " + filename +" val "+ val +"Sending to succ "+succ2);
				avd_fname_val = succ2 + ":" + filename + ":" + val;

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname_val);
			}
			//Thread.sleep(200);
		}catch (Exception e) {
			Log.e(TAG, "File write failed");
		}
		Log.v("insert", values.toString());
		return uri;
		// TODO Auto-generated method stub
	}


	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		portnumber = Integer.parseInt(myPort);
		myID=portStr;
		//recoveryflag=true;
		int[] avd_id = {5562,5556,5554,5558,5560};
		for(int i =0 ; i< avd_id.length;i++) {
			String hash_val = null;
			try {
				hash_val = genHash(String.valueOf(avd_id[i]));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			map_Hash.put(hash_val, String.valueOf(avd_id[i]));
			Nodes.add(hash_val);
			Collections.sort(Nodes);
		}
		map_Succ.put("5554","5558:5560");
		map_Succ.put("5556","5554:5558");
		map_Succ.put("5558","5560:5562");
		map_Succ.put("5560","5562:5556");
		map_Succ.put("5562","5556:5554");

		map_Pred.put("5554","5556:5562");
		map_Pred.put("5556","5562:5560");
		map_Pred.put("5558","5554:5556");
		map_Pred.put("5560","5558:5554");
		map_Pred.put("5562","5560:5558");

		String[] my_succ = map_Succ.get(myID).split(":");
		first_succ = Integer.parseInt(my_succ[0]);
		second_succ = Integer.parseInt(my_succ[1]);

		String[] my_pred = map_Pred.get(myID).split(":");
		first_pred = Integer.parseInt(my_pred[0]);
		second_pred = Integer.parseInt(my_pred[1]);

		String filename = "Filecheck";
		String filecontent = "File Created";
		//http://stackoverflow.com/questions/7817551/how-to-check-file-exist-or-not-and-if-not-create-a-new-file-in-sdcard-in-async-t
		try
		{
			String appPath = getContext().getApplicationContext().getFilesDir().getPath();
			System.out.println("App Path: " + appPath);
			File f = new File(appPath+"/"+filename);
			if(f.exists()) {
//				recoveryflag=true;
				System.out.println("File Already Exists");
				System.out.println("Successor Recovery Part");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "RecoverPart");
				System.out.println("Successful Retrieval both Pred and Succ");
//				Thread.sleep(3000);
//				recoveryflag=false;
			}
			else
			{
				System.out.println("File Not Found, Creating");
				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(filename, Context.MODE_PRIVATE));
				outputStreamWriter.write(filecontent);
				outputStreamWriter.close();
			}

			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket"+myPort);
			return false;
		} catch (NetworkOnMainThreadException e)
		{

		}
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		while(recoveryflag){
			System.out.println("Waiting For Recovery @Query");
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		System.out.println("Nodes list is " + Nodes);
		System.out.println("Query is "+selection);
		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
		try {

			System.out.println("Selection:"+selection);
				//System.out.println("Inside else");
				if(selection.equals("*"))
				{
					System.out.println("Selection is "+selection);
					System.out.println("Inside*:Add Self Files");
//					Thread.sleep(500);
					String appPath = getContext().getApplicationContext().getFilesDir().getPath();
					System.out.println("App Path: " + appPath);
					File path = new File(appPath);
					File[] files = path.listFiles();
					for (int i = 0; i < files.length; i++) {
					//	System.out.println("InsideFor:");

						if (files[i].isFile()) { //this line weeds out other directories/folders
					//		System.out.println("InsideIf2:");
							System.out.println(files[i]);
							String filedir = String.valueOf(files[i]);
							String[] filenamearr = filedir.split("/");
							String filename = filenamearr[5];
							System.out.println("filename is:"+filename);
							if(filename.equals("Filecheck"))
							{
								continue;
							}
							FileInputStream fis = getContext().openFileInput(filename);
							InputStreamReader isr = new InputStreamReader(fis);
							BufferedReader bufferedReader = new BufferedReader(isr);
							//StringBuilder sb = new StringBuilder();
							String p;
							//System.out.println("InsideWhile:");
							p = bufferedReader.readLine();
							System.out.println("P is:" + p);
							matrixCursor.addRow(new Object[]{filename, p});
						}
					}
					String sendtoall = "PerformAll";
					String strreturned = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendtoall,myID).get();
					System.out.println("Str Returned "+strreturned);
					String[] strarray =strreturned.split("-");
					int num = strarray.length;
					for(int i=0;i<num;i++)
					{
						String[] str =strarray[i].split(":");
						String fname = str[0].trim();
						String pval = str[1].trim();
						System.out.println("Fname:Before adding: " + fname);
						System.out.println("Pval:Before adding: " + pval);
						matrixCursor.addRow(new Object[]{fname, pval});
					}
				}
				if(selection.equals("@"))
				{
					System.out.println("Selection is "+selection);
					System.out.println("Inside@:Add Self Files");
//					Thread.sleep(500);
					String appPath = getContext().getApplicationContext().getFilesDir().getPath();
					//System.out.println("App Path: " + appPath);
					File path = new File(appPath);
					File[] files = path.listFiles();

					for (int i = 0; i < files.length; i++) {
					//	System.out.println("InsideFor:");
						if (files[i].isFile()) {
					//		System.out.println("InsideIf2:");
							System.out.println(files[i]);
							String filedir = String.valueOf(files[i]);
							String[] filenamearr = filedir.split("/");
							String filename = filenamearr[5];
							System.out.println("filename is:"+filename);
							if(filename.equals("Filecheck"))
							{
								continue;
							}
							FileInputStream fis = getContext().openFileInput(filename);
							InputStreamReader isr = new InputStreamReader(fis);
							BufferedReader bufferedReader = new BufferedReader(isr);
							//StringBuilder sb = new StringBuilder();
							String p;
							//System.out.println("InsideWhile:");
							p = bufferedReader.readLine();
							System.out.println("P is:" + p);
							matrixCursor.addRow(new Object[]{filename, p});
							System.out.println("Added to cursor"+filename+" : "+p);
						}
					}
				}
				if(!(selection.equals("*")) && !(selection.equals("@")))
				{
					System.out.println("Query is single query" + selection);
					System.out.println(selection);
					String filename = selection;
//					Thread.sleep(500);

					//System.out.println("Selection is "+selection);
					//String keyhash = genHash(filename);
					String node_Hash = null;
					String current_hash = genHash(filename);
					System.out.println("QueryingFile "+selection+" HashValue "+current_hash);
					Collections.sort(Nodes);
					for (int i = 0; i < Nodes.size(); i++) {
						String locHash = Nodes.get(i);

						if (locHash.compareTo(current_hash) >= 0) {
							node_Hash = locHash;
							break;

						}
						else{
							if (i== (Nodes.size()-1)){
								node_Hash=Nodes.get(0);
							}
						}
					}
					String avd = map_Hash.get(node_Hash);

					if(avd.equals(myID)) {

						System.out.println("Query is in myID "+myID+"Filename "+filename);
						FileInputStream fis = getContext().openFileInput(selection);
						InputStreamReader isr = new InputStreamReader(fis);
						BufferedReader bufferedReader = new BufferedReader(isr);
						String line, p = null;
						while ((line = bufferedReader.readLine()) != null) {
							p = line;
						}
						matrixCursor.addRow(new Object[]{filename, p});
					}
					else {
						String message2send = "QueryForward";
						String msg2send = "PresentInAvd:" + avd + ":Key:" + filename;
						String str = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2send, msg2send).get();
						Thread.sleep(100);
						if(str.equals(null))
						{

							String[] succ = map_Succ.get(avd).split(":");
							String succ1 = succ[0];
							String msg2succ = "PresentInAvd:" + succ1 + ":Key:" + filename;
							str = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2send, msg2succ).get();

						}
						String[] strarray = str.split(":");
						String fname = strarray[0].trim();
						String pval = strarray[1].trim();
						System.out.println("Fname:Before adding: " + fname);
						System.out.println("Pval:Before adding: " + pval);
						matrixCursor.addRow(new Object[]{fname, pval});
					}
			}
		} catch (Exception e) {
			Log.e(TAG, "File query failed");
		}
		return matrixCursor;
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
			while (true) {
				try {
					//    Log.v(TAG, "Server Socket");
					Socket leader = serverSocket.accept();
					InputStreamReader input = new InputStreamReader(leader.getInputStream());
					BufferedReader reader = new BufferedReader(input);
					//System.out.println("check message type");
					String Message;
					if ((Message = reader.readLine()) != null)                    //Read string from client side
					{
						System.out.println("Received at SERVER "+Message);
					//	System.out.println("Message not null");

		if(Message.contains("RetrieveMyFiles"))

						{

							recoveryflag = true;
							String[] retrieve = Message.split(":");
							String toreturn1 ="init:";
							int avd_id = Integer.parseInt(retrieve[1]);
							System.out.println("To retrieve files for avd_id"+avd_id);

							String appPath = getContext().getApplicationContext().getFilesDir().getPath();
							File path = new File(appPath);
							File[] files = path.listFiles();
							for (int i = 0; i < files.length; i++) {
						//		System.out.println("InsideFor:");
								if (files[i].isFile()) { //this line weeds out other directories/folders
									//System.out.println("InsideIf2:");
									System.out.println(files[i]);
									String filedir = String.valueOf(files[i]);
									String[] filenamearr = filedir.split("/");
									String filename = filenamearr[5];
									//System.out.println("filename is:"+filename);
									if(filename.equals("Filecheck"))
									{
										continue;
									}
									FileInputStream fis = getContext().openFileInput(filename);
									InputStreamReader isr = new InputStreamReader(fis);
									BufferedReader bufferedReader = new BufferedReader(isr);
									String key;
									key = bufferedReader.readLine();
									//System.out.println("check:Filename "+filename +" value "+key+  " belongs to "+BelongsTo(genHash(filename)) +" Comparing it with "+avd_id +" my id is "+myID);
									if(Integer.parseInt(BelongsTo(genHash(filename)))==avd_id)
									{
									//	System.out.println("check2:Filename "+filename +" value "+key+  " belongs to "+BelongsTo(genHash(filename)) +"compare successful");
										toreturn1 = toreturn1 + filename+"-"+key+":";
									}
								}
							}
							System.out.println("Return To Avd "+avd_id +"From Succ "+myID);
							System.out.println("FilesAre: "+toreturn1);
							PrintStream printer = new PrintStream(leader.getOutputStream());
							printer.println(toreturn1);
							printer.flush();
							recoveryflag=false;

						}
						if(Message.contains("RetrievePredecessorFiles"))
						{
							recoveryflag = true;
							String[] retrieve = Message.split(":");
							String toreturn1="init:";
							int avd_id = Integer.parseInt(retrieve[1]);
							//System.out.println("To retrieve files to avd_id"+avd_id);
							String appPath = getContext().getApplicationContext().getFilesDir().getPath();
							File path = new File(appPath);
							File[] files = path.listFiles();
							System.out.println("Retrieving predecessor files for "+avd_id);
							for (int i = 0; i < files.length; i++) {
							//	System.out.println("InsideFor:");
								if (files[i].isFile()) { //this line weeds out other directories/folders
							//		System.out.println("InsideIf2:");
									System.out.println(files[i]);
									String filedir = String.valueOf(files[i]);
									String[] filenamearr = filedir.split("/");
									String filename = filenamearr[5];
									//System.out.println("filename is:"+filename);
									if(filename.equals("Filecheck"))
									{
										continue;
									}
									FileInputStream fis = getContext().openFileInput(filename);
									InputStreamReader isr = new InputStreamReader(fis);
									BufferedReader bufferedReader = new BufferedReader(isr);
									String key;
									key = bufferedReader.readLine();
									//System.out.println("check3:Filename "+filename +" value "+key+ " belongs to "+BelongsTo(genHash(filename)) +" Comparing it with my id"+myID +" avd id is "+avd_id);
									if(Integer.parseInt(BelongsTo(genHash(filename)))==Integer.parseInt(myID))
									{
									//	System.out.println("check4:Filename "+filename +" value "+key+ " belongs to "+BelongsTo(genHash(filename))+"Check successful");
										toreturn1 = toreturn1 + filename+"-"+key+":";
									}
								}
							}
							PrintStream printer = new PrintStream(leader.getOutputStream());
							System.out.println("Return to Avd "+avd_id+"From Pred "+myID);
							System.out.println("FilesAre: "+toreturn1);
							printer.println(toreturn1);
							printer.flush();
							recoveryflag=false;
						}
						if(Message.equals("DeleteFile"))
						{
							String fname = reader.readLine();
							getContext().deleteFile(fname);
						//	System.out.println("Delete Successful");
						}
						if (Message.equals("InsertTo"))
						{
							while (recoveryflag){
								System.out.println("Waiting for recovery at Insertto");
								Thread.sleep(200);
							}
							String fname = reader.readLine();
							String val = reader.readLine();
							System.out.println(" INSERT(ServerSide):Key " + fname + " val "+" belongs to "+BelongsTo(genHash(fname))+ val+" :To be Stored In " + myID);
							OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(fname, Context.MODE_PRIVATE));
							outputStreamWriter.write(val.toString());
							outputStreamWriter.close();
							PrintStream printer = new PrintStream(leader.getOutputStream());             //Write over the socket
							String query_return = "InsertSuccessful";
							printer.println(query_return);
							System.out.println("Sent back Insert Successful Message " +myID );
							printer.flush();
						}
						if(Message.equals("QueryPresent"))
						{
							while (recoveryflag){
								System.out.println("Waiting for recovery at Insertto");
								Thread.sleep(200);
							}
							String filename = reader.readLine();
							System.out.println("query received at " + myID + " query is " + filename);

							FileInputStream fis = getContext().openFileInput(filename);
							InputStreamReader isr = new InputStreamReader(fis);
							BufferedReader bufferedReader = new BufferedReader(isr);
							String p;
							p = bufferedReader.readLine();
							PrintStream printer = new PrintStream(leader.getOutputStream());             //Write over the socket
							//String messageback = "MessageBack:";
							String query_return = filename + ":" + p;
							//printer.println(messageback);
							//printer.flush();
							printer.println(query_return);
							System.out.println("Sent back query result "+query_return );
							printer.flush();
							//matrixCursor.addRow(new Object[]{filename, p});
						}
						if(Message.equals("PerformStar"))
						{
							Thread.sleep(500);
							//System.out.println("Selection is "+selection);
							System.out.println("Inside: Perform Star");
							//String appPath = getContext().getApplicationContext().getFilesDir().getPath();
//							System.out.println("App Path: " + appPath);
//							File path = new File(appPath);
//							File[] files = path.listFiles();
//							int count = files.length;
							PrintStream printer = new PrintStream(leader.getOutputStream());
//							printer.println(count);
//							printer.flush();
							String[] Files = getContext().getFilesDir().list();
							printer.println("Alive");
							printer.flush();
							for(String file : Files)
							{
									FileInputStream fis = getContext().openFileInput(file);
									InputStreamReader isr = new InputStreamReader(fis);
									BufferedReader bufferedReader = new BufferedReader(isr);
									String p;
									p = bufferedReader.readLine();
									System.out.println("P is:" + p);
									printer.println(file+":"+p);
									printer.flush();
							}
							printer.close();
						}

					}
			} catch (IOException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, String> {
		@Override
		protected String doInBackground(String... msgs) {

			//int[] PORTS = {11108,11112,11116,11120,11124};                              //Array of all 5 port values
			try {
				//for (int i : myPortsList )                                                //Loop over all ports to multicast

				if(msgs[0].contains("Request2Join")) {
					System.out.println("Trying to send request to Leader to join port "+portnumber);
					//Log.e(TAG, "Inside ClientTask");
					Socket socket = null;
//                    while(!socket.isConnected())
//                    do {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), leaderport);
//                        stop=true;
//                    }while(!(socket.isConnected()));
					System.out.println("connected to leader");
					//Log.e(TAG, "Client Socket Created");


//                    String msg_request = msgs[0].trim();                                                         //Message to be sent
					String msg_request = "JOIN";
					PrintStream ps = new PrintStream(socket.getOutputStream());
					msg_request = msg_request + ":" + portnumber;                                           //Send message and sender portnumber
					System.out.println("MSG_REQUEST:" + msg_request);
					ps.println(msg_request);
//                    ps.flush();
					Thread.sleep(100);
					System.out.println("Sent request to leader");

				}
				if(msgs[0].equals("RecoverPart"))
				{
					System.out.println("Inside Client Task Recover Part");

					Recover();
				}
				if(msgs[0].equals("DeleteFrom"))
				{
					String[] avd_fname = msgs[1].split(":");
					String avd = avd_fname[0];
					String file = avd_fname[1];
					Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd)*2);
					PrintStream psc = new PrintStream(client.getOutputStream());
					String msg_client = "DeleteFile";
					psc.println(msg_client);
					psc.flush();
					psc.println(file);
					psc.flush();
				}
				if(msgs[0].equals("InsertIntoSucc"))
				{
					String[] avd_fname_val = msgs[1].split(":");
					String avd = avd_fname_val[0];
					String file = avd_fname_val[1];
					String val = avd_fname_val[2];
					for(int i=0;i<5;i++) {
						System.out.println("Insert Attempt "+i);
						Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd) * 2);
						PrintStream psc = new PrintStream(client.getOutputStream());
						String msg_client = "InsertTo";
						psc.println(msg_client);
						psc.flush();
						System.out.println("Sending File: " + file + " and Val " + val + " to avd " + avd);
						psc.println(file);
						psc.flush();
						psc.println(val);
						psc.flush();
						InputStreamReader input = new InputStreamReader(client.getInputStream());   //Read over the socket
						BufferedReader reader = new BufferedReader(input);
						String response = null;
						if ((response = reader.readLine())!=null && response.equals("InsertSuccessful")) {
							System.out.println("Insert is Successful ");
							break;
						}
					}

				}
				if(msgs[0].equals("QueryForward"))
				{
					while(recoveryflag){
						System.out.println("Waiting for recovery to finish at QueryForward");
						Thread.sleep(200);
					}
					String msg_query = msgs[1].trim();                                                         //Message to be sent
					String[] msg_query_array = msg_query.split(":");
					String presentinavd = msg_query_array[1];
					String keytoquery = msg_query_array[3];
					System.out.println("Present in Avd"+presentinavd+"Key to Query "+keytoquery);
					int QueryPort = Integer.parseInt(presentinavd)*2;
					Collections.sort(Nodes);

					Socket Queryclient = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), QueryPort);
					PrintStream psq = new PrintStream(Queryclient.getOutputStream());
					String query_client = "QueryPresent";
					psq.println(query_client);
					psq.flush();
					System.out.println("Sending Message"+query_client+" and key "+keytoquery+" to "+presentinavd);
					psq.println(keytoquery);
					psq.flush();
//					Thread.sleep(1000);
					InputStreamReader input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
					BufferedReader reader = new BufferedReader(input);
					String query_return = null;
					queryisreturned=false;
					if((query_return = reader.readLine())!= null) {
						System.out.println("Received from " + QueryPort + " message " + query_return);
						String[] query_array = query_return.split(":");
						String fname = query_array[0];
						String p_val = query_array[1];
						System.out.println("Fname returned:" + fname + "Pval returned:" + p_val);
						queryisreturned = true;
						return fname + ":" + p_val;
					}
					else {
						//map_Succ.get(presentinavd);
						String[] succ = map_Succ.get(presentinavd).split(":");
						String succ1 = succ[0];
						String succ2 = succ[1];
						QueryPort = Integer.parseInt(succ1) * 2;
						Queryclient = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), QueryPort);
						psq = new PrintStream(Queryclient.getOutputStream());
						query_client = "QueryPresent";
						psq.println(query_client);
						psq.flush();
						System.out.println("Sending Message" + query_client + " and key " + keytoquery + " to " + presentinavd);
						psq.println(keytoquery);
						psq.flush();
						Thread.sleep(100);
						input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
						reader = new BufferedReader(input);
						query_return = null;
						queryisreturned = false;
						if ((query_return = reader.readLine()) != null) {
							System.out.println("Received from " + QueryPort + " message " + query_return);
							String[] query_array = query_return.split(":");
							String fname = query_array[0];
							String p_val = query_array[1];
							System.out.println("Fname returned:" + fname + "Pval returned:" + p_val);
							queryisreturned = true;
							return fname + ":" + p_val;
						}
						else {
							QueryPort = Integer.parseInt(succ2) * 2;
							Queryclient = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), QueryPort);
							psq = new PrintStream(Queryclient.getOutputStream());
							query_client = "QueryPresent";
							psq.println(query_client);
							psq.flush();
							System.out.println("Sending Message" + query_client + " and key " + keytoquery + " to " + presentinavd);
							psq.println(keytoquery);
							psq.flush();
							Thread.sleep(100);
							input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
							reader = new BufferedReader(input);
							query_return = null;
							queryisreturned = false;
							if ((query_return = reader.readLine()) != null) {
								System.out.println("Received from " + QueryPort + " message " + query_return);
								String[] query_array = query_return.split(":");
								String fname = query_array[0];
								String p_val = query_array[1];
								System.out.println("Fname returned:" + fname + "Pval returned:" + p_val);
								queryisreturned = true;
								return fname + ":" + p_val;
							}
						}
					}
				}
				if(msgs[0].equals("PerformAll"))
				{
					List<String> tosend = new ArrayList<String>();
					for(String key : map_Hash.keySet())
					{
						tosend.add(map_Hash.get(key));
						System.out.println("To send list is (before)"+tosend);
					}
					tosend.remove(myID);
					System.out.println("To send list is (after)"+tosend);
					//int total_count=0;
					String toreturn="firsttime";
					for(int j=0;j<tosend.size();j++) {

						int socket = Integer.parseInt(tosend.get(j))*2;
						System.out.println("Sending to"+socket/2);
						System.out.println("Performing Star on " + socket/2 + " Originated by " + msgs[1]);
						Socket Queryclient = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), socket);
						PrintStream psq = new PrintStream(Queryclient.getOutputStream());
						String query_client = "PerformStar";
						psq.println(query_client);
						psq.flush();
						System.out.println("Sending Message" + query_client);
						Thread.sleep(100);

						InputStreamReader input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
						BufferedReader reader = new BufferedReader(input);
//						int count = Integer.parseInt(reader.readLine());

//						for (int i = 0; i < count; i++) {
//							String recv = reader.readLine();
//							String[] recvarr = recv.split(":");
//							String fname = recvarr[0];
//							String p_val = recvarr[1];
//							System.out.println("Fname returned:" + fname + "Pval returned:" + p_val);
//							if(toreturn.equals("firsttime"))
//								toreturn = fname + ":" + p_val + "-";
//							else
//								toreturn = toreturn + fname + ":" + p_val + "-";
//							System.out.println("To return " + toreturn);
//						}
						String recv;
						if((recv = reader.readLine())!=null)
						{
						while((recv = reader.readLine())!=null)
						{
							String[] recvarr = recv.split(":");
							String fname = recvarr[0];
							String p_val = recvarr[1];
							System.out.println("Fname returned:" + fname + "Pval returned:" + p_val);
							if(toreturn.equals("firsttime"))
							{
								toreturn = fname + ":" + p_val + "-";
							}
							else
							{
								toreturn = toreturn + fname + ":" + p_val + "-";
							}

						}
						}
						else
						{
							continue;
						}

						//total_count = total_count + count;
					}
					System.out.println("To return " + toreturn);
					return toreturn;
				}

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public void Recover(){
			try {
				recoveryflag=true;
				System.out.println("RecoveryStart");
				System.out.println("Starting Recovery: First Successor");

				Socket succ1_client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), first_succ * 2);
				PrintStream psc_succ1 = new PrintStream(succ1_client.getOutputStream());
				String msg_succ1 = "RetrieveMyFiles:" + myID;
				psc_succ1.println(msg_succ1);
				psc_succ1.flush();
//				Thread.sleep(100);
				InputStreamReader inputs1 = new InputStreamReader(succ1_client.getInputStream());   //Read over the socket
				BufferedReader readers1 = new BufferedReader(inputs1);
				String kvs1;
				if ((kvs1 = readers1.readLine()) != null) {
					String[] keyvals1 = kvs1.split(":");
					for (int i = 1; i < keyvals1.length; i++) {
						String[] keyvalarrs1 = keyvals1[i].split("-");
						System.out.println("RECOVERED from 1st succ: "+first_succ+" for "+myID+" file "+keyvals1[i]);
						OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(keyvalarrs1[0], Context.MODE_PRIVATE));
						outputStreamWriter.write(keyvalarrs1[1]);
						outputStreamWriter.close();
					}
				}
				else {
					System.out.println("Starting Recovery: Second Successor");
					Socket succ2_client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), second_succ * 2);
					PrintStream psc_succ2 = new PrintStream(succ2_client.getOutputStream());
					String msg_succ2 = "RetrieveMyFiles:" + myID;
					psc_succ2.println(msg_succ2);
					psc_succ2.flush();
//					Thread.sleep(100);
					InputStreamReader inputs2 = new InputStreamReader(succ2_client.getInputStream());
					BufferedReader readers2 = new BufferedReader(inputs2);
					String kvs2;
					if ((kvs2 = readers2.readLine()) != null) {
						String[] keyvals2 = kvs2.split(":");
						for (int i = 1; i < keyvals2.length; i++) {
							String[] keyvalarrs2 = keyvals2[i].split("-");
							System.out.println("RECOVERED from 2nd succ: "+second_succ+" for "+myID+" file "+keyvals2[i]);
							OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(keyvalarrs2[0], Context.MODE_PRIVATE));
							outputStreamWriter.write(keyvalarrs2[1]);
							outputStreamWriter.close();
						}
					}
				}
				System.out.println("Predecessor Recovery Part");

				System.out.println("Starting Recovery: First Predecessor");
				Socket pred1_client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), first_pred * 2);
				PrintStream psc_pred1 = new PrintStream(pred1_client.getOutputStream());
				String msg_pred1 = "RetrievePredecessorFiles:" + myID;
				psc_pred1.println(msg_pred1);
				psc_pred1.flush();
//				Thread.sleep(100);
				InputStreamReader inputp1 = new InputStreamReader(pred1_client.getInputStream());
				BufferedReader readerp1 = new BufferedReader(inputp1);
				String kvp1;
				if ((kvp1 = readerp1.readLine()) != null) {
					System.out.println("Received from pr1 :"+first_pred +" "+kvp1);
					String[] keyvalp1 = kvp1.split(":");
					System.out.println("Length of recovery msg is "+keyvalp1.length);
						for (int i = 1; i < keyvalp1.length; i++) {
							String[] keyvalarrp1 = keyvalp1[i].split("-");
							System.out.println("RECOVERED from 1st pred: " + keyvalp1[i]);
							OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(keyvalarrp1[0], Context.MODE_PRIVATE));
							outputStreamWriter.write(keyvalarrp1[1]);
							outputStreamWriter.close();
						}

				}
				System.out.println("Starting Recovery: Second Predecessor");
				Socket pred2_client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), second_pred * 2);
				PrintStream psc_pred2 = new PrintStream(pred2_client.getOutputStream());
				String msg_pred2 = "RetrievePredecessorFiles:" + myID;
				psc_pred2.println(msg_pred2);
				psc_pred2.flush();
//				Thread.sleep(100);
				InputStreamReader inputp2 = new InputStreamReader(pred2_client.getInputStream());   //Read over the socket
				BufferedReader readerp2 = new BufferedReader(inputp2);
				String kvp2;
				if ((kvp2 = readerp2.readLine()) != null) {
					System.out.println("Received from pr2 :"+second_pred +" "+kvp2);
					String[] keyvalp2 = kvp2.split(":");
					System.out.println("Length of recovery msg is "+keyvalp2.length);
					for (int i = 1; i < keyvalp2.length; i++) {
						String[] keyvalarrp2 = keyvalp2[i].split("-");
						System.out.println("RECOVERED from 2nd pred: "+keyvalp2[i]);
						OutputStreamWriter outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(keyvalarrp2[0], Context.MODE_PRIVATE));
						outputStreamWriter.write(keyvalarrp2[1]);
						outputStreamWriter.close();
					}
				}
//
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				System.out.println("RecoveryEnd");

				recoveryflag=false;

			}
		}

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
}

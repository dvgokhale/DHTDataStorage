package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//Packet 1
//Failure handling packets are from 10
public class SimpleDynamoProvider extends ContentProvider {
	String[] mapper = {"5554", "33d6357cfaaf0f72991b0ecd8c56da066613c089", "5556", "208f7f72b198dadd244e61801abe1ec3a4857bc9", "5558", "abf0fd8db03e5ecb199a9b82929e9db79b909643", "5560", "c25ddd596aa7c81fa12378fa725f706d54325d12", "5562", "177ccecaec32c54b82d5aaafc18a2dadb753e3b1"};
	String idHash = null;
	FileOutputStream fileStream = null;
	int position;
	String tempStr = "";
	static int msgCounter = 0;
	String[] keySet = new String[400];
	public int keySetItr = 0;
	FileOutputStream globalOutputStream ;
	static String fileName = "";
	ConcurrentHashMap<String, Value> dataAggregatorMap = new  ConcurrentHashMap<String, Value>(400);

	static ConcurrentHashMap<String, String> dataMap = new ConcurrentHashMap<String, String>(400);
	static ConcurrentHashMap<String, Integer> NodePosMap = new  ConcurrentHashMap<String, Integer>(400);
	
	//Arrangement of AVDs in the ring
	String[] arrangement = {"177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "208f7f72b198dadd244e61801abe1ec3a4857bc9", "33d6357cfaaf0f72991b0ecd8c56da066613c089", "abf0fd8db03e5ecb199a9b82929e9db79b909643", "c25ddd596aa7c81fa12378fa725f706d54325d12"};
	
	//This stores the key value pairs
	static ConcurrentHashMap<String, Value> map = new ConcurrentHashMap<String, Value>(400);
	static ConcurrentHashMap<String, String> replicatedKeys = new ConcurrentHashMap<String, String>(400);
	static ConcurrentHashMap<String, String> msgIDMap = new ConcurrentHashMap<String, String>(400);
	
	long upSinceTime;
	
	public static PriorityBlockingQueue<String> processingQ = new PriorityBlockingQueue<String>(20, new Comparator<String>() {
		@Override
		public int compare(String lhs, String rhs) {
			return 0;
		}
	});
	public static PriorityBlockingQueue<JSONObject> respQ = new PriorityBlockingQueue<JSONObject>(20, new Comparator<JSONObject>() {
		@Override
		public int compare(JSONObject lhs, JSONObject rhs) {
			return 0;
		}
	});
	public static PriorityBlockingQueue<JSONObject> processingQOne = new PriorityBlockingQueue<JSONObject>(20, new Comparator<JSONObject>() {
		@Override
		public int compare(JSONObject lhs, JSONObject rhs) {
			return 0;
		}
	});
	public static PriorityBlockingQueue<JSONObject> processingQTwo = new PriorityBlockingQueue<JSONObject>(20, new Comparator<JSONObject>() {
		@Override
		public int compare(JSONObject lhs, JSONObject rhs) {
			return 0;
		}
	});
	public static Queue<JSONObject> dataQ = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQThree = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQFour = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQFive = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQEleven = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQTwelve = new ConcurrentLinkedQueue<JSONObject>();
	public static Queue<JSONObject> processingQTwentyOne = new ConcurrentLinkedQueue<JSONObject>();
	String id = null;
	String port = null;
	boolean nodeReady = false;
	int printTime = 1;
	long timeOutValue = 3L;
	long hardTimeOutValue = 5L;
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		try {
			if(selection.equals("*") || selection.equals("@"))
			{
				Log.d("ZDelete","Deleted the current AVD's contents");
				map.clear();
				if(selection.equals("*"))
				{
					Log.d("ZDelete","Intimating other AVDs to delete their contents");
					for(int i = 0; i < 5; i++)
					{
						if(i == position)
						{
							continue;
						}
						JSONObject packet = constructPacket21(arrangement[i], "*");
						respQ.add(packet);
					}
				}
			}
			else {
				Value mapOutput = hashMapGet(selection);
				if (mapOutput == null) {
					Log.d("ZDelete", "Not present key:" + selection);
					String dataNode = findKeyNode(selection);
					String[] succS = findSuccessors(dataNode);
					JSONObject packet = constructPacket21(dataNode, selection);
					respQ.add(packet);
					packet = constructPacket21(succS[0], selection);
					respQ.add(packet);
					packet = constructPacket21(succS[1], selection);
					respQ.add(packet);
				} else {
					Log.d("ZDelete", "deleted Key: " + selection + " Now asking the successors to delete this key");
					String[] succS = findSuccessors(idHash);
					JSONObject packet = constructPacket21(succS[0], selection);
					respQ.add(packet);
					packet = constructPacket21(succS[1], selection);
					respQ.add(packet);
					map.remove(selection);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Set<Map.Entry<String, Object>> s = values.valueSet();
		Object value = null;
		Object keyValue = null;
		Iterator itr = s.iterator();
		try {
			while (itr.hasNext()) {
				Map.Entry me = (Map.Entry) itr.next();
				value = me.getValue();
				me = (Map.Entry) itr.next();
				keyValue = me.getValue();
				Log.d("ZContentProvider", "Key:" + keyValue.toString() + ", values:" + value.toString());				
			}
			
			String rightNode = findKeyNode(keyValue.toString());
			String[] successors = findSuccessors(rightNode);
			long currTime = System.currentTimeMillis();
			if (rightNode.equals(idHash)) {
				insertIntoMap(keyValue.toString(), value.toString(), currTime);
				Log.d("ZContentProvider", "Key:" + keyValue.toString() + " Inserted into current AVD");
			} 
			else {
				JSONObject packet = constructPacket1(keyValue.toString(), value.toString(), currTime, false, rightNode, "");
				respQ.add(packet);
			}
			JSONObject packet;
			if(successors[0].equals(idHash))
			{
				Log.d("ZInsert", "Current AVD is a successor node, inserting key: " + keyValue.toString() + " here");
				insertIntoMap(keyValue.toString(), value.toString(),currTime);
			}
			else {
				packet = constructPacket1(keyValue.toString(), value.toString(), currTime, true, successors[0], rightNode);
				respQ.add(packet);
			}
			if(successors[1].equals(idHash))
			{
				Log.d("ZInsert", "Current AVD is a successor node, inserting key: " + keyValue.toString() + " here");
				insertIntoMap(keyValue.toString(), value.toString(),currTime);
			}
			else {
				packet = constructPacket1(keyValue.toString(), value.toString(), currTime, true, successors[1], rightNode);
				respQ.add(packet);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return uri;
	}
	public boolean insertIntoDataAggregatorMap(String key, String value, long currTime)
	{
		int different = 0;
		Value oldVal = null;
		Value newVal = new Value(value,currTime);
		int toBeInserted = 1;
		Log.d("ZInsertAggregatorMap", "Inserting key: " + key);
		if (dataAggregatorMap.containsKey(key)) {
			Log.d("ZInsertAggregatorMap", "Updating the value of key: " + key);
			oldVal = dataAggregatorMap.get(key);
			different = 1;
			if(currTime < oldVal.version)
			{
				toBeInserted = 0;
			}
		}
		if(toBeInserted == 1) {
			dataAggregatorMap.put(key, newVal);
		}
		if (different == 1) {
			if (!((oldVal.value).equals(value))) {
				Log.d("ZInsertAggregatorMap", "Values differed");
			}
		}
		return true;
	}

	public boolean insertIntoMap(String key, String value, long currTime) throws NoSuchAlgorithmException, JSONException, IOException {
		String keyNode = findKeyNode(key);
		String[] predS = findPredecessors(idHash);
		Log.d("ZInsertIntoMap", "The key:" + key + " belongs to node: " + mapNodeIdToId(keyNode));
		if(keyNode.equals(idHash))
		{
			Log.d("ZInsertIntoMap", "The key:" + key + " belongs to the current node");
		}
		else if(keyNode.equals(predS[0]) || keyNode.equals(predS[1])) {
			Log.d("ZInsertIntoMap", "The key:" + key + " belongs to one of the predecessors");
			Log.d("ZInsertIntoMap", "Inserting key:" + key + " to replication table");
			replicatedKeys.put(key, keyNode);
		}
		else
		{
			Log.d("ZInsertIntoMap", "Error: Shouldn't be here: Rogue key detected: " + key + " which belongs to node: " + mapNodeIdToId(keyNode));
			// Have to check here
			if(replicatedKeys.containsKey(key))
			{
				Log.d("ZInsertIntoMap", "Error: Shouldn't be here");
				replicatedKeys.remove(key);
			}
			return false;
		}

		int different = 0;
		Value oldVal = null;
		Value newVal = new Value(value,currTime);
		int toBeInserted = 1;
		Log.d("ZInsertIntoMap", "Inserting key: " + key);
		if (map.containsKey(key)) {
			Log.d("ZInsertIntoMap", "Updating the value of key: " + key);
			oldVal = hashMapGet(key);
			different = 1;
			if(currTime < oldVal.version)
			{
				toBeInserted = 0;
			}
		}
		if(toBeInserted == 1) {
			map.put(key, newVal);
		}
		if (different == 1) {
			if (!((oldVal.value).equals(value))) {
				Log.d("ZInsertIntoMap", "Values differed");
			}
		}
		printHashMapContents();
		return true;
	}

	public JSONObject constructPacket21(String node, String key) throws JSONException, IOException, NoSuchAlgorithmException
	{
		JSONObject record = new JSONObject();
		record.put("messageType", 21);
		record.put("key", key);
		record.put("from", id);
		record.put("to", Integer.parseInt(mapNodeIdToId(node)) * 2 + "");
		record.put("toId", mapNodeIdToId(node));
		return record;
	}

	//Delete packet
	public boolean type21PacketProcessor(JSONObject req) throws JSONException, IOException, NoSuchAlgorithmException
	{
		Log.d("Ztype21PacketProcessor", "Deleting this key: " + req.getString("key"));
		if(req.getString("key").equals("*"))
		{
			map.clear();
		}
		else
		{
			if(map.containsKey(req.getString("key"))) {
				map.remove(req.getString("key"));
			}
			else
			{
				Log.d("Ztype21PacketProcessor", "Couldn't delete key: " + req.getString("key")+ " Key not present in the AVD");
			}
		}
		return true;
	}

	public JSONObject constructPacket1(String key, String value, long currTime, boolean replication, String to, String rightNode) throws JSONException, IOException, NoSuchAlgorithmException {
		JSONObject record = new JSONObject();
		record.put("messageType", 1);
		record.put("key", key);
		record.put("value", value);
		record.put("from", id);
		record.put("fromHash", idHash);
		record.put("keyHash", genHash(key));
		record.put("version", currTime);
		record.put("to", Integer.parseInt(mapNodeIdToId(to)) * 2 + "");
		if (replication)
		{
			record.put("replication", true);
			record.put("replicatedFrom", rightNode);
		}
		else
		{
			record.put("replication", false);
		}
		return record;
	}

	public boolean type1PacketProcessor(JSONObject req) throws IOException, JSONException, NoSuchAlgorithmException {
		insertIntoMap(req.getString("key"), req.getString("value"), req.getLong("version"));
		return true;
	}

	public JSONObject packetDelegatorOne() throws JSONException, IOException, NoSuchAlgorithmException, InterruptedException {
		Log.d("ZProcessor", "Initialized Packet Delegator");
		while (true) {
			JSONObject req = null;
			req = processingQOne.take();
			if (req != null) {
				switch (req.getInt("messageType")) {
				case 1:
					type1PacketProcessor(req);
					break;

				case 2:
					//The @ result handler
					type2PacketProcessor(req);
					break;

				case 3:
					//Return specific node keys
					type3PacketProcessor(req);
					break;

				default:
					Log.d("ZPacketHandler", "Invalid packet type:");

				}
			}
		}
	}

	//delete packets start from 21
	public JSONObject packetDelegatorTwo() throws JSONException, IOException, NoSuchAlgorithmException, InterruptedException {
		Log.d("ZProcessor", "Initialized Packet Delegator");
		while (true) {
			JSONObject req = null;
			req = processingQTwo.take();
			if (req != null) {
				switch (req.getInt("messageType")) {
				case 4:
					//The @ result provider
					type4PacketProcessor(req);
					break;

				case 5:
					//The @ result handler
					type5PacketProcessor(req);
					//Log.d("ZPacketDelegator", "Exited type5PacketProcessor");
					break;

				case 11:
					//Return specific node keys
					type11PacketProcessor(req);
					break;

				case 12:
					//Handler of response of specific node keys
					type12PacketProcessor(req);
					break;

				case 21:
					//Delete handler
					type21PacketProcessor(req);
					break;
				default:
					Log.d("ZPacketHandler", "Invalid packet type:");
				}
			}
		}
	}

	public boolean type5PacketProcessor(JSONObject req) throws JSONException {
		if (msgIDMap.get(req.getString("messageId")) != null) {
			msgIDMap.remove(req.getString("messageId"));
			Log.d("ZType5PacketProcessor", "Removed this messageId: " +  req.getString("messageId"));
			Log.d("ZType5PacketProcessor", "Processed this read message: " + req.toString());
			dataMap.put(req.getString("messageId"), req.toString());		
		} else {
			Log.d("ZType5PacketProcessor", "Have already processed this read message: " + req.toString());
		}
		return true;
	}

	public boolean type4PacketProcessor(JSONObject req) throws JSONException {
		Date start1 = new Date();
		Date startTime = new Date();
		long timeElapsed = 0L;
		while(nodeReady == false)
		{
			Date end1 = new Date();
			if(end1.getTime() - start1.getTime() > printTime * 1000 )
			{
				timeElapsed = (end1.getTime() - startTime.getTime())/1000;
				Log.d("ZType4PacketProcessor", "Waiting for the node to be ready: Querying for @. MessageId: " + req.getString("messageId") + " For " + timeElapsed + " secs");
				start1 = new Date();
			}
		}
		Log.d("ZType4PacketProcessor", "Node ready: Querying for @.  MessageId: " + req.getString("messageId"));
		JSONObject resp = new JSONObject();
		JSONArray results = getHashMapContents(null);
		resp.put("messageType", 5);
		resp.put("to", req.getString("fromIdPort"));
		resp.put("toId", req.getString("fromId"));
		resp.put("fromId", id);
		resp.put("messageId", req.getString("messageId"));
		resp.put("results", results);
		respQ.add(resp);
		return true;
	}

	public String findKeyNode(String key) throws NoSuchAlgorithmException, IOException, JSONException {
		String keyHash = genHash(key);
		if (keyHash.compareTo(arrangement[arrangement.length - 1]) > 0) {
			return arrangement[0];
		}
		for (int i = 0; i < arrangement.length; i++) {
			if (keyHash.compareTo(arrangement[i]) < 0) {
				Log.d("ZFindKeyNode", "Node found: " + arrangement[i] + " : " + mapNodeIdToId(arrangement[i])  + " key: " + key);
				return arrangement[i];
			}
		}
		Log.d("ZFindKeyNode", "Node not found for key: " + key);
		return "";
	}

	public String[] findPredecessors(String node) throws JSONException, NoSuchAlgorithmException, IOException {
		String[] neighbors = new String[2];
		int nodePos = NodePosMap.get(node);
		neighbors[0] = arrangement[(nodePos - 1 + 5) % 5];
		neighbors[1] = arrangement[(nodePos - 2 + 5) % 5];
		return neighbors;
	}

	public String[] findSuccessors(String node) throws JSONException, NoSuchAlgorithmException, IOException {
		String[] neighbors = new String[2];
		int nodePos = NodePosMap.get(node);
		neighbors[0] = arrangement[(nodePos + 1) % 5];
		neighbors[1] = arrangement[(nodePos + 2) % 5];
		return neighbors;
	}

	public boolean serverSide() throws IOException, JSONException {
		Log.d("ZServerSide", "Initialized Server side");
		ServerSocket serverSocket = new ServerSocket(10000);
		String msg = "";
		try
		{
			while (true)
			{
				Socket socket = serverSocket.accept();
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				socket.setSoTimeout(1000);
				msg = br.readLine();
				PrintWriter pw = new PrintWriter(socket.getOutputStream());
				pw.println("Received");
				pw.flush();
				Log.d("ZServerSide Received:", msg + "");
				br.close();
				pw.close();
				if(msg != null) {
					JSONObject req = new JSONObject(msg);
					switch (req.getInt("messageType")) {
					case 1:
					case 2:
					case 3:
						processingQOne.add(req);
						break;
					default:
						processingQTwo.add(req);
					}
				}
				else
				{
					Log.e("ZServerSide","Received: NULL");
				}
			}
		}
		catch(SocketTimeoutException e)
		{
			Log.d("ZServerSide", "Socket timeout");
		}
		return true;
	}

	public boolean clientSide() throws IOException, JSONException, InterruptedException {
		Log.d("ZClientSide", "Initialized Client side");
		while (true)
		{
			JSONObject resp = null;
			resp = respQ.take();
			if (resp != null)
			{
				Log.d("ZClientSide", "Sending:" + resp.toString());
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(resp.getString("to")));
				PrintWriter out = new PrintWriter(socket.getOutputStream());
				out.println(resp.toString());
				out.flush();
				socket.setSoTimeout(500);
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				try
				{
					String ack = br.readLine();
					Log.d("ZClientSide: Ack:", "" + ack);
					if(ack == null)
					{
						Log.d("ZClientSide:", "Ack is null for message: " + resp.toString());
					}
				}
				catch(SocketTimeoutException e)
				{
					Log.d("ZClientSide", "Socket timeout");
				}
				finally {
					br.close();
					out.close();
					socket.close();
				}
			}
		}
	}

	public String mapNodeIdToId(String id) throws IOException, JSONException, NoSuchAlgorithmException {
		for (int i = 0; i < 10; i++) {
			if (mapper[i].equals(id)) {
				return mapper[i - 1];
			}
		}
		return null;
	}

	public boolean acquireProperState() throws IOException, JSONException, NoSuchAlgorithmException {
		Log.d("ZRevertToProperState", ".................................Started State Acquisition.......................................");
		querySuccPred();
		Log.d("ZRevertToProperState", ".................................Finishd State Acquisition.......................................");
		return true;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		id = portStr;
		try {
			idHash = genHash(id);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		port = myPort;
		for (int i = 0; i < arrangement.length; i++) {
			if (idHash.equals(arrangement[i])) {
				position = i;
				break;
			}
		}
		//Initializations
		for( int i =0; i < arrangement.length; i++)
		{
			NodePosMap.put(arrangement[i],i);
		}

		upSinceTime = System.currentTimeMillis();
		// This is the correct system state
		dataAggregatorMap.clear();
		dataMap.clear();
		replicatedKeys.clear();
		msgIDMap.clear();
		respQ.clear();
		processingQ.clear();
		processingQOne.clear();
		processingQTwo.clear();
		processingQThree.clear();
		processingQFour.clear();
		processingQFive.clear();
		processingQEleven.clear();
		processingQTwelve.clear();
		processingQTwentyOne.clear();
		dataQ.clear();
		map.clear();

		Log.d("ZSimpleDynamo", "About to start server on Async");
		new ServerThread().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
		new Thread(new Runnable() {
			public void run() {
				try {
					Log.d("ZSimpleDynamo", "Starting client..");
					clientSide();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				try {
					packetDelegatorOne();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				try {
					try {
						packetDelegatorTwo();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}).start();

		try {
			String time = upSinceTime + "";
			File file = new File(getContext().getFilesDir(), "appAccessTime");
			if (file.exists()) {
				Log.d("ZOnCreate", "File present: AVD had Failed");
				Log.d("ZOnCreate", "Map size: " + map.size());
				Log.d("ZOnCreate", "keySetItr: " + keySetItr);
				new Thread(new Runnable() {
					public void run() {
						try {
							try {
								acquireProperState();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}
					}
				}).start();
			}
			else
			{
				Log.d("ZOnCreate", "File not present: AVD had not Failed");
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput("appAccessTime", Context.MODE_PRIVATE);
				outputStream.write("dummy".getBytes());
				outputStream.close();
				nodeReady = true;
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return true;
	}

	public JSONObject constructPacket2(String key, String to, String msgId) throws IOException, JSONException, NoSuchAlgorithmException {
		JSONObject resp = new JSONObject();
		resp.put("messageType", 2);
		resp.put("key", key);
		resp.put("to", Integer.parseInt(mapNodeIdToId(to)) * 2 + "");
		resp.put("toId", Integer.parseInt(mapNodeIdToId(to)) + "");
		resp.put("fromId", id);
		resp.put("fromIdPort", Integer.parseInt(mapNodeIdToId(idHash)) * 2 + "");
		resp.put("messageId", msgId);
		msgIDMap.put(msgId, "yes");
		return resp;
	}

	public boolean type3PacketProcessor(JSONObject req) throws IOException, JSONException, NoSuchAlgorithmException {
		if (msgIDMap.containsKey(req.getString("messageId")))
		{
			Log.d("ZType3PacketProcessor", "Removed this messageId: " + req.getString("messageId"));
			Log.d("ZType3PacketProcessor", "Processed this read message: " + req.toString());
			dataMap.put(req.getString("messageId"), req.toString());
		}
		else
		{
			Log.d("ZType3PacketProcessor", "Have already processed this read message: " + req.toString());
		}
		return true;
	}

	public Value hashMapGet(String selection)
	{
		return map.get(selection);
	}

	public boolean type2PacketProcessor(JSONObject req) throws IOException, JSONException, NoSuchAlgorithmException {
		if(nodeReady == true)
		{
			Value value = hashMapGet(req.getString("key"));
			if (value != null) {
				Log.d("Ztype2PacketProcessor","Record found for key:" + req.getString("key"));
				JSONObject resp = new JSONObject();
				resp.put("messageType", 3);
				resp.put("key", req.getString("key"));
				resp.put("to", req.getString("fromIdPort"));
				resp.put("toId", req.getString("fromId"));
				resp.put("fromId", id);
				resp.put("fromIdPort", Integer.parseInt(mapNodeIdToId(idHash)) * 2 + "");
				resp.put("messageId", req.getString("messageId"));
				resp.put("value", value.value);
				resp.put("version", value.version);
				respQ.add(resp);
			}
			else
			{
				Log.d("ZType2PacketProcessor", "Shouldn't be here, this key is not present in this AVD key: " +req.getString("key") + " , key belongs to node: " + findKeyNode(req.getString("key")));
			}
			return true;
		}
		else
		{
			Date start1 = new Date();
			Date startTime = new Date();
			long timeElapsed = 0L;
			while(nodeReady == false)
			{
				Date end1 = new Date();
				if(end1.getTime() - start1.getTime() > printTime * 1000 )
				{
					timeElapsed = (end1.getTime() - startTime.getTime())/1000;
					Log.d("ZType2PacketProcessor", "Shouldn't be here, but it's ok. Waiting for the node to be ready. Key: " + req.getString("key") + " for " + timeElapsed + " secs");
					start1 = new Date();
				}
			}
			Log.d("ZType2PacketProcessor", "Shouldn't be here, but it's ok. Done waiting for the node to acquire state. Key: " + req.getString("key"));
			type2PacketProcessor(req);
		}
		return true;
	}

	public String[] findDataNodes() {
		String nodes[] = new String[2];
		//Find the current node's position in the chord
		nodes[0] = arrangement[(position + 2) % 5];
		nodes[1] = arrangement[(position + 3) % 5];
		return nodes;
	}

	public JSONObject constructType4Packet(String to, String msgId) throws JSONException, IOException, NoSuchAlgorithmException {
		JSONObject resp = new JSONObject();
		resp.put("messageType", 4);
		resp.put("to", Integer.parseInt(mapNodeIdToId(to)) * 2 + "");
		resp.put("toId", Integer.parseInt(mapNodeIdToId(to)) + "");
		resp.put("fromId", id);
		resp.put("fromIdPort", port);
		resp.put("key", "@");
		resp.put("messageId",msgId);
		return resp;
	}

	public boolean getDataAggregatorMapContents(MatrixCursor result) throws JSONException {
		Iterator it = dataAggregatorMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			//Log.d("ZDataAggregContents: ", pair.getKey() + " = " + pair.getValue());
			if(result != null) {
				result.addRow(new Object[]{pair.getKey(), ((Value) pair.getValue()).value});
			}
		}
		return true;
	}

	public JSONArray getHashMapContents(MatrixCursor result) throws JSONException {
		if(result == null)
		{
			Log.d("ZGetHashMapContents", "Result null passed");
		}
		Iterator it = map.entrySet().iterator();
		JSONArray arr = new JSONArray();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			if(result != null) {
				result.addRow(new Object[]{pair.getKey(), ((Value) pair.getValue()).value});
			}
			JSONObject obj = new JSONObject();
			obj.put("key", pair.getKey().toString());
			obj.put("value", ((Value) pair.getValue()).value);
			obj.put("version", ((Value) pair.getValue()).version);
			arr.put(obj);
		}
		Log.d("ZGetHashMapContents", "Arr contents: " + arr.toString());
		return arr;
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
	String[] selectionArgs, String sortOrder) {
		try {
			//updateFile();
			Log.d("ZQuery", "Querying for: " + selection);
			String columns[] = {"key", "value"};
			MatrixCursor result = new MatrixCursor(columns);
			Date start1 = new Date();
			Date startTime = new Date();
			long timeElapsed = 0L;
			while(nodeReady == false)
			{
				Date end1 = new Date();
				if(end1.getTime() - start1.getTime() > printTime * 1000 )
				{
					timeElapsed = (end1.getTime() - startTime.getTime())/1000;
					Log.d("ZQuery", "Waiting for the node to be ready, querying for: " + selection + " For " + timeElapsed +" secs");
					start1 = new Date();
				}
			}
			if (selection.equals("@") || selection.equals("*"))
			{
				printHashMapContents();
				JSONArray arr = getHashMapContents(result);

				if(selection.equals("@")) {
					Log.d("ZQuery ", "Result for @: " + result.toString());
				}
				if (selection.equals("*"))
				{
					Log.d("ZQuery", "Got * as input");
					String[] msgIds = new String[5];
					String[] dataNodes = findDataNodes();
					int l =0;
					for (int i = 0; i < 5; i++) {
						if(i != position) {
							String msgId = id + "." + msgCounter++;
							msgIds[l++] = msgId;
							msgIDMap.put(msgId, "yes");
							JSONObject packet = constructType4Packet(arrangement[i], msgId);//, false);
							respQ.add(packet);
						}
					}
					//Querying all
					//Store your results
					int counter = 0;
					JSONObject temp = null;
					for (int i = 0; i < arr.length(); i++) {
						temp = arr.getJSONObject(i);
						insertIntoDataAggregatorMap(temp.getString("key"),temp.getString("value"),temp.getLong("version"));
					}
					int processedAVDs = 0;
					start1 = new Date();
					startTime = new Date();
					Log.d("ZQuery*", "Stuck in query *");
					timeElapsed = 0L;
					while (true && counter != 4){// && timeElapsed < timeOutValue) {
						Date end1 = new Date();
						if(end1.getTime() - start1.getTime() > printTime * 1000 )
						{
							timeElapsed = (end1.getTime() - startTime.getTime())/1000;
							Log.d("ZQuery*", "Stuck in query * , stuck for about: " + timeElapsed + " secs" );
							start1 = new Date();
						}
						JSONArray resArr = null;
						for(int j = 0; j < 4; j++) {
							if (dataMap.containsKey(msgIds[j])) {
								String toBeJSONised = dataMap.get(msgIds[j]);
								JSONObject partResults =  new JSONObject(toBeJSONised);
								resArr = partResults.getJSONArray("results");
								Log.d("ZQuery*Result size", resArr.length() + arr.length() + "");
								int i = 0;

								for (i = 0; i < resArr.length(); i++) {
									temp = resArr.getJSONObject(i);
									insertIntoDataAggregatorMap(temp.getString("key"),temp.getString("value"),temp.getLong("version"));
								}
								dataMap.remove(msgIds[j]);
								counter ++;
								processedAVDs++;
							}
						}
						//Compensate for lack of failure detection
						if(counter == 3)
						{
							//Wait for some time X before getting out
							Date start = new Date();
							Date end = new Date();
							while(end.getTime() - start.getTime() < 2.5 * 1000 ){
								end = new Date();
							}
							int foundFlag = 0;
							for(int k = 0; k < 4; k++) {
								if (dataMap.containsKey(msgIds[k])) {
									foundFlag = 1;
									Log.d("ZQuery*", "Got the last of the data after waiting");
									break;
								}
							}
							if(foundFlag == 0) {
								//Bail, data not in possession, AVD has failed
								Log.d("ZQuery*", "Done with waiting, one AVD is probably down");
								counter = 4;
							}
						}
					}
					getDataAggregatorMapContents(result);
					//remove dataAggregator contents
					dataAggregatorMap.clear();
					Log.d("ZQuery*", "Stuck in query *: Done");
					Log.d("ZQuery*", "Number of actual dataNodes processed: " + processedAVDs);
					Log.d("ZFinal ", result.toString());
				}
				Log.d("ZQuery", "Processed @ & * requests, returning the results");
				return result;
			} else {
				Log.d("ZQueryRetrieval", selection);
				Log.d("ZQueryRetrieval", "Key: " + selection + " : " + hashMapGet(selection));
				Value mapOutput = hashMapGet(selection);
				if (mapOutput == null) {
					String rightNode = findKeyNode(selection);
					String[] successors = findSuccessors(rightNode);
					String msgId = id + "." + msgCounter++;
					msgIDMap.put(msgId,"yes");
					JSONObject packet = null;
					if(!idHash.equals(rightNode)) {
						packet = constructPacket2(selection, rightNode, msgId);
						respQ.add(packet);
					}
					if(!idHash.equals(successors[0])) {
						packet = constructPacket2(selection, successors[0], msgId);
						respQ.add(packet);
					}
					if(!idHash.equals(successors[1])) {
						packet = constructPacket2(selection, successors[1], msgId);
						respQ.add(packet);
					}
					//Get Query
					start1 = new Date();
					startTime = new Date();
					Log.d("ZQuery", "Stuck in querying specific key: " + selection);
					timeElapsed = 0L;
					while (true && timeElapsed < hardTimeOutValue) {
						Date end1 = new Date();
						if(end1.getTime() - start1.getTime() > printTime * 1000 )
						{
							timeElapsed = (end1.getTime() - startTime.getTime())/1000;
							Log.d("ZQuery", "Stuck in querying some specific key: " + selection + " , stuck for about: " + timeElapsed + " secs" );
							start1 = new Date();
						}
						if (dataMap.containsKey(msgId)) {
							Log.d("ZQuery", "Wait ended, got the value for key: " + selection);
							String output = dataMap.get(msgId);
							dataMap.remove(selection);
							JSONObject JSONisedOutput = new JSONObject(output);
							result.addRow(new Object[]{selection, JSONisedOutput.getString("value")});
							Log.d("ZResultQuery", "key: " + selection + " value: " + JSONisedOutput.getString("value"));
							Log.d("ZQuery", "Stuck in querying some specific key: " + selection + " Done...........................");
							return result;
						}
					}
					if(timeElapsed > hardTimeOutValue)
					{
						Log.d("ZQuery", "TimedOut, MessageId: "+ msgId + " key: " + selection);
					}
				} else {
					result.addRow(new Object[]{selection, mapOutput.value});
					Log.d("ZResultQuery", "key: " + selection + " value: " + mapOutput.value + " version: " + mapOutput.version);
					return result;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		Log.d("ZQuery", "Shouldn't be here. Timed out, querying for key: " + selection);
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
	String[] selectionArgs) {
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

	public boolean printHashMapContents() throws JSONException, NoSuchAlgorithmException, IOException {
		Log.d("ZPrinting", "---------------------------------Hash Map contents/-----------------------");
		Log.d("ZHashMapSize", map.size() + "");
		Iterator it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			Log.d("ZPairPrint: ", pair.getKey() + " = " + ((Value)pair.getValue()).value + " Version: " + ((Value)pair.getValue()).version);
		}
		Log.d("ZReplicatedMapSize", replicatedKeys.size() + "");
		return true;
	}

	private class ServerThread extends AsyncTask<Void, Void, Void> {
		@Override
		protected Void doInBackground(Void... params) {
			try {
				serverSide();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public boolean type12PacketProcessor(JSONObject req) throws IOException, JSONException, NoSuchAlgorithmException {
		String msgId = req.getString("messageId");
		if (msgIDMap.get(msgId) != null) {
			Log.d("ZType12PacketProcessor", "Processed this read message: " + req.toString());
			dataMap.put(msgId, req.getJSONArray("data").toString());
			JSONArray resArr = new JSONArray(req.getJSONArray("data").toString());
			JSONObject temp = null;
			if(!(resArr.length() == 0))
			{
				int i;
				for (i = 0; i < resArr.length(); i++) {
					temp = resArr.getJSONObject(i);
					insertIntoMap(temp.getString("key"), temp.getString("value"), temp.getLong("version"));
				}
			}
		}
		else
		{
			Log.d("ZType12PacketProcessor", "Have already processed this read message: " + req.toString());
		}
		return true;
	}

	public boolean type11PacketProcessor(JSONObject req) throws IOException, JSONException, NoSuchAlgorithmException {
		Iterator it = map.entrySet().iterator();
		String reqNode = req.getString("reqNode");
		Date start1 = new Date();
		Date startTime = new Date();
		long timeElapsed = 0L;
		while(nodeReady == false)
		{
			Date end1 = new Date();
			if(end1.getTime() - start1.getTime() > printTime * 1000 )
			{
				timeElapsed = (end1.getTime() - startTime.getTime())/1000;
				Log.d("ZType11PacketProcessor", "Stuck in Querying for records of node: " + req.getString("reqNodeId") + " Node not ready. For " + timeElapsed +" secs");
				start1 = new Date();
			}
		}
		Log.d("ZType11PacketProcessor", "Node ready. Querying for records of node: " + req.getString("reqNodeId"));
		printHashMapContents();
		JSONArray arr = new JSONArray();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			JSONObject obj = new JSONObject();
			obj.put("key", pair.getKey().toString());
			obj.put("value", ((Value)pair.getValue()).value);
			obj.put("version", ((Value)pair.getValue()).version);
			String[] predS = findPredecessors(idHash);
			if (replicatedKeys.containsKey(pair.getKey())) {
				if (reqNode.equals(replicatedKeys.get(pair.getKey()))) {
					arr.put(obj);
				}
				else
				{
					// Some code can sit here for further optimization
				}
			}
			else if(reqNode.equals(idHash))
			{
				//Give your own data
				arr.put(obj);
			}
			else if(reqNode.equals(predS[0]) || reqNode.equals(predS[1]))
			{
				if(!(findKeyNode((String)pair.getKey()).equals(idHash)))
				{
					Log.d("ZType11", "Shouldn't be here");
				}
			}
			else
			{
				String keyNode = findKeyNode((String)pair.getKey());
				if(keyNode.equals(predS[0]) || keyNode.equals(predS[1]))
				{
					replicatedKeys.put((String)pair.getKey(),keyNode);
					if(reqNode.equals(predS[0]) || reqNode.equals(predS[1]))
					{
						arr.put(obj);
					}
					else
					{
						Log.d("ZType11","Wrong logic, shouldn't be here.");
					}

				}
				else
				{
					Log.d("ZType11","What's happening? Really shouldn't be here.");
				}
			}
		}
		//Send the results back
		JSONObject resp = new JSONObject();
		resp.put("messageType", 12);
		resp.put("messageId", req.getString("messageId"));
		resp.put("to", req.getString("fromIdPort"));
		resp.put("toId", req.getString("fromId"));
		resp.put("fromId", id);
		resp.put("fromIdPort", port);
		resp.put("data", arr);
		respQ.add(resp);
		return true;
	}

	public JSONObject constructPacket11(String to, String whichNode, String msgId) throws JSONException, IOException, NoSuchAlgorithmException {
		JSONObject packet = new JSONObject();
		packet.put("messageType", 11);
		packet.put("reqNode", whichNode);
		packet.put("reqNodeId", mapNodeIdToId(whichNode));
		packet.put("to", Integer.parseInt(mapNodeIdToId(to)) * 2 + "");
		packet.put("toId", Integer.parseInt(mapNodeIdToId(to)) + "");
		packet.put("fromId", id);
		packet.put("messageId",msgId);
		packet.put("fromIdPort", port);
		return packet;
	}

	public String queryForFirstPredData(String node1, String node2) throws NoSuchAlgorithmException, JSONException, IOException {
		String msgId = id + "." + msgCounter++;
		JSONObject packet = constructPacket11(node1, node2, msgId);
		respQ.add(packet);
		Log.d("ZQueryForFirstPredData", "packet: " + packet.toString());
		packet = constructPacket11(node2, node2, msgId);
		respQ.add(packet);
		msgIDMap.put(msgId, "yes");
		Log.d("ZQueryForFirstPredData", "packet: " + packet.toString());
		return msgId;
	}

	public String queryForSecondPredData(String node1, String node2) throws NoSuchAlgorithmException, JSONException, IOException {
		String msgId = id + "." + msgCounter++;
		JSONObject packet = constructPacket11(node1, node1, msgId);
		Log.d("ZQueryForSecondPredData", "packet: " + packet.toString());
		respQ.add(packet);
		packet = constructPacket11(node2, node1, msgId);
		respQ.add(packet);
		Log.d("ZQueryForSecondPredData", "packet: " + packet.toString());
		msgIDMap.put(msgId, "yes");
		return msgId;
	}

	public String queryForOwnData(String node1, String node2) throws NoSuchAlgorithmException, JSONException, IOException {
		String msgId = id + "." + msgCounter++;
		JSONObject packet = constructPacket11(node1, idHash, msgId);
		Log.d("ZQueryForOwnData", "packet: " + packet.toString());
		respQ.add(packet);
		packet = constructPacket11(node2, idHash, msgId);
		respQ.add(packet);
		Log.d("ZQueryForOwnData", "packet: " + packet.toString());
		msgIDMap.put(msgId, "yes");
		return msgId;
	}

	public boolean querySuccPred() throws NoSuchAlgorithmException, JSONException, IOException {
		String[] succS = findSuccessors(idHash);
		String[] predS = findPredecessors(idHash);
		String msgId1 = queryForFirstPredData(succS[0], predS[0]);
		String msgId2 = queryForSecondPredData(predS[1], predS[0]);
		String msgId3 = queryForOwnData(succS[0], succS[1]);
		int counter = 0;
		int totalRecords = 0;
		Date start1 = new Date();
		Date startTime = new Date();
		Log.d("ZQuerySuccPred", "Stuck in data acquisition after failure");
		long timeElapsed = 0L;
		while ((true && counter < 5) && timeElapsed < timeOutValue)
		{
			Date end1 = new Date();
			if(end1.getTime() - start1.getTime() > printTime * 1000 )
			{
				timeElapsed = (end1.getTime() - startTime.getTime())/1000;
				Log.d("ZQuerySuccPred", "Stuck in data acquisition after failure, stuck for about: " + timeElapsed + " secs" );
				start1 = new Date();
			}
			JSONArray resArr = null;
			JSONObject temp = null;
			String msgId;
			if (dataMap.containsKey(msgId1) || dataMap.containsKey(msgId2) || dataMap.containsKey(msgId3)) {
				Log.d("ZQuerySuccPred", "Got 1/3 of the data");
				Log.d("ZQuerySuccPred", "Sizes, Map: " + map.size());
				if (dataMap.containsKey(msgId1)) {
					msgId = msgId1;
				} else if (dataMap.containsKey(msgId2)) {
					msgId = msgId2;
				} else {
					msgId = msgId3;
				}
				dataMap.remove(msgId);
				++counter;
			}
		}
		if(timeElapsed > timeOutValue)
		{
			Log.d("ZQuerySuccPred", "TimedOut");
		}
		Log.d("ZQuerySuccPred", "Stuck in data acquisition after failure: Done........................................");
		Log.d("ZQuerySuccPred", "Node is back up and fully functional");
		
		nodeReady = true;
		Log.d("ZPrinting", "---------------------------------Hash Map contents/-----------------------");
		Log.d("ZPrinting Size:", map.size() + "");

		Iterator it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			Log.d("ZPrinting Pair: ", pair.getKey() + " = " + ((Value)pair.getValue()).value + " Version: " + ((Value)pair.getValue()).version);
		}
		Log.d("ZPrinting", "---------------------------------/Hash Map contents-----------------------");

		return true;
	}
}

//When the AVD comes back up again there is no version control

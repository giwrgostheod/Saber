package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.client;

public class InputStreamTuple {
	
	private long 	timestamp; /* Event timestamp: long */
	private long 	userIdMSB; /* User ID:         uuid */ 
	private long 	userIdLSB;   
	private long 	pageIdMSB; /* Page ID:         uuid */ 
	private long 	pageIdLSB;
	private long 	  adIdMSB; /* Ad ID:           uuid */ 
	private long 	  adIdLSB;
	private int 	   adType; /* Ad Type: 	         int*/
	private int  	eventType; /* Event Type:        int*/
	private int  	ipAddress; /* IP Address: 	     int*/
	private int 	  queryId;
	
	public InputStreamTuple () {
		/* Initialise all attributes */
		reset ();
	}
	
	private void reset () {
		this.timestamp	           = 0L;		
		this.userIdMSB             = 0L;
		this.userIdLSB             = 0L;
		this.pageIdMSB             = 0L;
		this.pageIdLSB			   = 0L;
		this.adIdMSB               = 0L;
		this.adIdLSB 			   = 0L;
		this.adType 			   = 0;
		this.eventType			   = 0;
		this.ipAddress			   = 0;
		this.queryId 		       = 0;

	}
	
	public static void parse (String line, InputStreamTuple t) {
		String [] s = line.split(",");
		/* assert s.length == 15: "error: invalid line format" */
		t.timestamp                = Long.parseLong  (s[0]); //+ 1; /* Start time from 1 rather 0 */
		t.userIdMSB                = Long.parseLong  (s[1]);
		t.userIdLSB            	   = Long.parseLong  (s[2]);
		t.pageIdMSB                = Long.parseLong  (s[3]);
		t.pageIdLSB                = Long.parseLong  (s[4]);
		t.adIdMSB             	   = Long.parseLong  (s[5]);
		t.adIdLSB                  = Long.parseLong  (s[6]);
		t.adType                   = Integer.parseInt(s[7]);
		t.eventType                = Integer.parseInt(s[8]);
		t.ipAddress				   = Integer.parseInt(s[9]);
		t.queryId                  = Integer.parseInt(s[10]);
	}
	
	public long  getTimestamp()                  { return             timestamp; }
	public long  getUserIdMSB()                  { return             userIdMSB; }
	public long  getUserIdLSB ()                 { return             userIdLSB; }
	public long  getPageIdMSB()                  { return             pageIdMSB; }
	public long  getPageIdLSB()                  { return             pageIdLSB; }
	public long  getAdIdMSB()                    { return 	            adIdMSB; }
	public long  getAdIdLSB()                    { return               adIdLSB; }
	public int   getAdType()     	             { return                adType; }
	public int   getEventType()                  { return             eventType; }
	public int   getIpAddress()                  { return             ipAddress; }
	public int   getQueryId()                    { return               queryId; }

	
}


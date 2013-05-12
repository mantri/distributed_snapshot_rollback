{-
  Project 1 
	CS 380D (53640)
	DISTRIBUTED COMPUTING I
	Spring 2013
	
	Name: Hemanth Kumar Mantri
	UTEID: HM7787
	CS Login: mantri
	
	Note: This implementation has all parts from Part-1 to Part-8 of the assignment.
	      I enabled logging only for SNAPSHOT and ROLLBACK related stuff.
	      Please wait till the end where you see "ROLLBACK: Rolled back Channel-43/34"
	      before terminating the process. It takes considerable time but definitely not
	      too long with the current snapshot and rollback Rwait() times.
	
	References: I referred the web for Chandy-Lamport's algorithm summaries to understand it better.
-}

{- Definition of Process class:
   id         - Process ID (0, 1, 2, 3, 4)
   in:ins     - List of input channels
   out:outs   - List of output channels
   snap:snaps - List of snapshot channels that store the snapshot state
 -}

def class Process(id, in:ins, out:outs, snap:snaps) =

	-- mutable variables
	val timeStamp  = Ref(0)       -- logical timeStamp
	val state      = Ref(0)       -- state of how much is written to and read from channels (bank balance)
	val snapState  = Ref(0)       -- recorded state snapshot
	val color      = Ref("white") -- Process turns red AFTER recording its state
	val rollback   = Ref(false)   -- becomes true after process has done roll back to previous snapshot
  
	-- Semaphores for mutex and synchronization
	-- Not all are actually needed except rSem and tSem
	val rSem      = Semaphore(1)   -- to avoid new reads during doRound() 
	val tSem      = Semaphore(1)   -- protect 'timeStamp'
	val sSem      = Semaphore(1)   -- protect 'state'
	val snSem     = Semaphore(1)   -- protect 'snapState'
	val cSem      = Semaphore(1)   -- protect 'color'
	val rollSem   = Semaphore(1)   -- protect 'rollback'
  
	-------- Routines to atomically get the class variables -----------
 
  	def getTimeStamp() =
  		tSem.acquire() >> timeStamp? >time> tSem.release() >> time
  	
  	def getState() =
  		sSem.acquire() >> state? >st> sSem.release() >> st
  	
  	def getSnapState() =
  		snSem.acquire() >> snapState? >snapSt> snSem.release() >> snapSt
  	
  	def getColor() =
  		cSem.acquire() >> color? >col> cSem.release() >> col
  	
  	def getRollback() =
  		rollSem.acquire() >> rollback? >roll> rollSem.release() >> roll
  	--------------------------------------------------------------------

  	------- Routines to atomically update the class variables ----------

	def updateTimestamp(recvdTime) =
		tSem.acquire() >>
			timeStamp := max(timeStamp?, recvdTime) + 1 >>
  		tSem.release()
 
	-- If processors were banks, add for Reads and subtract for writes (bank gets money => increase balance)
	-- However, I am adding for writes (project specification for Part-6 needs this)
 	def updateState(num, true) = sSem.acquire() >> state := state? + num >> sSem.release()
 	def updateState(num, false) = sSem.acquire() >> state := state? - num >> sSem.release()
    
   	def restoreState() =
   		sSem.acquire() >>
   			state := getSnapState() >> 
   			updateColor("white") >> -- needed for subsequent snapshots (if any)
   		sSem.release()
 
	def updateSnapState(num) =
  		snSem.acquire() >> 
  			snapState := num >> 
  		snSem.release()

	def updateColor(newColor) =
  		cSem.acquire() >> 
  			color := newColor >> 
  		cSem.release()
  	
 	def updateRollback(newval) =
  		rollSem.acquire() >>
  			rollback := newval >>
  		rollSem.release()
    
  	-- Increment the local timeStamp and create a message pair (timeStamp, value). 
  	def createMessage(value) =
  		tSem.acquire() >>
  			timeStamp := timeStamp? + 1 >>
  			timeStamp? >time>
  		tSem.release() >>
  		(time, value)
  	-------------------------------------------------------------------
  	
	-- Prepend contents of 'src' channel to 'dst' channel
	-- In our case we will prepend the snapshot channel contents
	-- To the original input channel during rollback step
	def restoreChannel(src, dst) =
		val temp = Channel()
		dst.getAll() >orig> putInChannel(orig, temp) >>
  		src.getAll() >fields> putInChannel(fields, dst) >>
  		temp.getAll() >saved> putInChannel(saved, dst) >>
  		src.getAll() -- NOTE: empty the snapshot channel to be used for subsequent snapshots

	def putInChannel([], _) = signal
	def putInChannel(elem:elems, dst) =
  		dst.put(elem) >> putInChannel(elems, dst)

	-- sum the values in a list of (time, value) pairs. Used for debugging channel states 
	def sum([]) = 0 
	def sum((a,b):xs) = b + sum(xs)
  
  	-- puts a random message after waiting for random time on to the output channel
  	-- NOTE: It has only write() permissions to the channel as discussed by Prof. Misra
	def sendAtRandomTime(write, chId, num) =
  		Rwait(Random(51)) >>
		createMessage(num) >message>
  		write(message) >>
  		updateState(num, true)
  		--Println("Process"+id + " wrote "+message+" on to channel " + chId + "; state = " + getState())

	-- send the marker without any wait  
	def sendNow(write, chId, marker) =
		createMessage(marker) >mark> write(mark)
		--Println("Process"+id + " wrote "+mark+" on to channel " + chId + "; state = " + getState())

	-- put messages on all outgoing channels (barrier sync: return once all are done)
	def putOutMessages([], _) = signal
	def putOutMessages((outCh,chId):outs, num) =
		(sendAtRandomTime(outCh.put, chId, num) ,  putOutMessages(outs, num)) >> signal

	{- send snapshot marker:
  		 1. Record state (so change color to red)
  	 	 2. Send markers to all outgoing channels
	-}
	def sendSnapMarker() =
		updateColor("red") >> updateSnapState(getState()) >>
		Println("SNAPSHOT: Saved Process-"+id + " state:" + getSnapState()) >> 
		putOutMessages(out:outs, 2013) -- 2013 is marker for SNAPSHOT message

   {- recv snapshot marker on channel 'in':
  	 if (state not recorded) {
  	 	1. record state of 'in' as NULL set;
  	 	2. sendMarker()
  	 } else {
  	 	1. record state of 'in' as all messages arrived after the state was recorded
  	 	   and before recving this marker
  	 }
   -}
	def recvSnapMarker(in, sn, chId) =
		Println("Snapshot marker received on channel-"+chId) >>
		(Ift(getColor() = "red") >> recvSnapRed(in, sn, chId) ; recvSnapWhite(in, sn, chId))
 
 	def recvSnapRed(in, sn, chId) = 
  		sn.getAll() >msgs> 
  		Println("red: SNAPSHOT: Value of saved Channel-"+chId+" state:"+ sum(msgs)) >>
  		putInChannel(msgs, sn) -- this puts back the msgs into snapshot channel
  	
  	def recvSnapWhite(in, sn, chId) =
  		sn.getAll() >msgs> 
  		Println("white: SNAPSHOT: Value of saved Channel-"+chId+" state:"+ sum(msgs)) >>
  		sendSnapMarker()
  	
	--recv rollback marker (for restoring to earlier snapshot) on channel 'in':
	def recvRollMarker(in, sn, chId) =
		Ift(getRollback() = false) >> recvRollFirst(in, sn, chId) ; recvRollSecond(in, sn, chId)
  	
	def recvRollFirst(in, sn, chId) =
  		updateRollback(true) >>
  		Println("first: ROLLBACK: Rollback marker received on Channel-"+chId) >>
  		restoreState() >> 
  		Println("ROLLBACK: Rolled back state of Process-"+id+" state:"+getState()) >>
 		restoreChannel(sn, in) >> 
  		Println("ROLLBACK: Rolled back Channel-"+chId) >>
  		putOutMessages(out:outs, 2014)  -- 2014 is marker for ROLLBACK message
  
  	def recvRollSecond(in, sn, chId) =
  		Println("second: ROLLBACK: Rollback marker received on Channel-"+chId) >>
  		restoreChannel(sn, in) >>
  		Println("ROLLBACK: Rolled back Channel-"+chId)

  	-- Recv a regularmessage
  	-- If process is RED, store the message in snapshot channel
	def recvMessage(remoteTime, value, sn, chId) =
		(if getColor() = "red" then sn.put((remoteTime, value)) else signal) >>
		updateTimestamp(remoteTime) >>
		updateState(value, false) >>
		doRound()
  
	-- see what kind of message did we receive (Thanks to John for cleaning this)
	def processMessage(remoteTime,  2013, in, sn, chId) = recvSnapMarker(in, sn, chId)
	def processMessage(remoteTime,  2014, in, sn, chId) = recvRollMarker(in, sn, chId)
	def processMessage(remoteTime, value, in, sn, chId) = recvMessage(remoteTime, value, sn, chId)

	-- gets a message from input channel and starts a round.
	-- No messages are processed by a process during its round
	def getMessage(in, sn, chId) =
		in.get() >(remoteTime,value)>
		--Println("Process"+id+" read ("+remoteTime+","+value+") from channel "+chId+ "; state = " + getState()) >>
  		rSem.acquire() >> 
  			processMessage(remoteTime, value, in, sn, chId) >> 
  		rSem.release() >>
  		getMessage(in, sn, chId)

	-- Note that snId and chId should be same for a given pair
	def getInMessages([], _) = stop
	def getInMessages((inCh, chId):ins, (snapCh, snId):snaps) =
		(getMessage(inCh, snapCh, chId) , getInMessages(ins, snaps))

	-- each round puts messages on output channel list  
	def doRound() =
		Random(101) >num> putOutMessages(out:outs, num)
  
	-- part7: initiate global snapshot. Called by process0. Marker value is 2013
	def initSnapshot() =
		rSem.acquire() >>
			updateSnapState(getState()) >> updateColor("red") >>
			Println("SNAPSHOT: Saved Process-"+id + " state:" + getSnapState()) >>
			putOutMessages(out:outs, 2013) >>
		rSem.release()
  	
  	-- part8: initiate rollback to previous snapshot. Called by process0. Marker value is 2014
  	def initRollback() =
  		rSem.acquire() >> 
  			restoreState() >> updateRollback("true") >> 
  			Println("ROLLBACK: Rolled back state of Process-"+id+" state:"+getState()) >>
  			putOutMessages(out:outs, 2014) >>
  		rSem.release()

	-- each slave does this in a loop
	def doRead() =
		getInMessages(in:ins, snap:snaps)

	-- master writes messages for the first time and then acts same as slave then on
	def master() = 
  		doRound()
  	
  	def slave() =
  		doRead()

stop
------------------------------- Process Class Ends --------------------------

--create (channel,channel_id) pairs. 
-- first part the of the ID indicates the write end and second part is read end
-- NOTE: Channel ID is needed for debugging purposes
val ch01 = (Channel(), "01")
val ch02 = (Channel(), "02")
val ch12 = (Channel(), "12")
val ch13 = (Channel(), "13")
val ch14 = (Channel(), "14")
val ch21 = (Channel(), "21")
val ch23 = (Channel(), "23")
val ch24 = (Channel(), "24")
val ch30 = (Channel(), "30")
val ch34 = (Channel(), "34")
val ch40 = (Channel(), "40")
val ch43 = (Channel(), "43")

-- snapshot channels
val sn01 = (Channel(), "sn01")
val sn02 = (Channel(), "sn02")
val sn12 = (Channel(), "sn12")
val sn13 = (Channel(), "sn13")
val sn14 = (Channel(), "sn14")
val sn21 = (Channel(), "sn21")
val sn23 = (Channel(), "sn23")
val sn24 = (Channel(), "sn24")
val sn30 = (Channel(), "sn30")
val sn34 = (Channel(), "sn34")
val sn40 = (Channel(), "sn40")
val sn43 = (Channel(), "sn43")

{-
 - Create 5 processes shown in the network
 - Inputs: channel Id, input channels, output channels, snapshot channels
-}
val proc0 = Process(0, [ch30,ch40], [ch01,ch02], [sn30,sn40])
val proc1 = Process(1, [ch01,ch21], [ch12,ch13,ch14], [sn01,sn21])
val proc2 = Process(2, [ch02,ch12], [ch21,ch23,ch24], [sn02,sn12]) 
val proc3 = Process(3, [ch13,ch23,ch43], [ch30,ch34], [sn13,sn23,sn43])
val proc4 = Process(4, [ch14,ch24,ch34], [ch40,ch43], [sn14,sn24,sn34])

{-
 - GOAL EXPRESSION:
 - Process0 is master/initiator. others are slaves. Process0 initiates global snapshot after some time.
 - Process0 should also initiate a rollback request after certain time that "we" think is good enough.
 - NOTE: Please change the Rwait() times accordingly.
 -      a. snapshot after 100ms : stay here
 -      b. snapshot after 1000ms: get a coffee break
 -      c. snapshot after 5-10s:  get a lunch break
-}
proc0.master() >> (
					proc0.slave() 
					| proc1.slave() 
					| proc2.slave() 
					| proc3.slave() 
					| proc4.slave() 
					| ((Rwait(100) >> 
						Println("ALERT: Process0 initiating snapshot .......") >>
						proc0.initSnapshot()))
					| ((Rwait(3000) >> 
						Println("ALERT: Process0 initiating rollback .......") >>
						proc0.initRollback()))
					)

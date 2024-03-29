/// Asynchronous Network Layer for Event-Driven Clients or Servers
// - this unit is a part of the Open Source Synopse mORMot framework 2,
// licensed under a MPL/GPL/LGPL three license - see LICENSE.md

unit mormot.net.async_rw;

interface

{$I mormot.defines.inc}

uses
  sysutils,
  classes,
  mormot.core.base,
  mormot.core.os,
  mormot.core.data,
  mormot.core.text,
  mormot.core.buffers,
  mormot.core.threads,
  mormot.core.log,
  mormot.core.rtti,
  mormot.net.sock;
//  mormot.net.server; // for multi-threaded process


{ ******************** Low-Level Non-blocking Connections }

type
  TAsyncConnectionHandle = type integer;

  TBlackObj = class
    count:Cardinal;
    lasttime:Cardinal;
  end;

  /// store information of one TPollAsyncSockets connection
  {$ifdef USERECORDWITHMETHODS}
  TPollSocketsSlot = record
  {$else}
  TPollSocketsSlot = object
  {$endif USERECORDWITHMETHODS}
  public
    /// the associated TCP connection
    // - equals 0 after TPollAsyncSockets.Stop
    socket: TNetSocket;
    /// Lock/Unlock R/W thread acquisition (lighter than a TRTLCriticalSection)
    lockcounter: array[boolean] of integer;
    /// the last error reported before the connection ends
    lastWSAError: TNetResult;
    /// the current read data buffer of this slot
    readbuf: RawByteString;
    /// the current write data buffer of this slot
    writebuf: RawByteString;
    /// acquire an exclusive R/W access to this connection
    // - returns true if slot has been acquired
    // - returns false if it is used by another thread
    // - warning: this method is not re-entrant
    function Lock(writer: boolean): boolean;
    /// try to acquire an exclusive R/W access to this connection
    // - returns true if slot has been acquired
    // - returns false if it is used by another thread, after the timeoutMS period
    // - warning: this method is not re-entrant
    function TryLock(writer: boolean; timeoutMS: cardinal): boolean;
    /// release exclusive R/W access to this connection
    procedure UnLock(writer: boolean);
  end;

  TMqttLoginResult = (
    mmqttError,
    mmqttException,
    mmqttTimeout,
    mmqttok);
  /// points to thread-safe information of one TPollAsyncSockets connection
  PPollSocketsSlot = ^TPollSocketsSlot;

  /// let TPollAsyncSockets.OnRead shutdown the socket if needed
  TPollAsyncSocketOnRead = (
    sorContinue,
    sorClose);

  {$M+}
  /// read/write buffer-oriented process of multiple non-blocking connections
  // - to be used e.g. for stream protocols (e.g. WebSockets or IoT communication)
  // - assigned sockets will be set in non-blocking mode, so that polling will
  // work as expected: you should then never use direclty the socket (e.g. via
  // blocking TCrtSocket), but rely on this class for asynchronous process:
  // OnRead() overriden method will receive all incoming data from input buffer,
  // and Write() should be called to add some data to asynchronous output buffer
  // - connections are identified as TObject instances, which should hold a
  // TPollSocketsSlot record as private values for the polling process
  // - ProcessRead/ProcessWrite methods are to be run for actual communication:
  // either you call those methods from multiple threads, or you run them in
  // loop from a single thread, then define a TSynThreadPool for running any
  // blocking process (e.g. computing requests answers) from OnRead callbacks
  // - inherited classes should override abstract OnRead, OnClose, OnError and
  // SlotFromConnection methods according to the actual connection class
  TPollAsyncSockets = class
  protected
    fRead: TPollSockets;
    fWrite: TPollSockets;
    fReadCount: integer;
    fWriteCount: integer;
    fReadBytes: Int64;
    fWriteBytes: Int64;
    fProcessing: integer;
    fWriteDirect:boolean;
    function GetCount: integer;
    // warning: abstract methods below should be properly overriden
    // return low-level socket information from connection instance
    function SlotFromConnection(connection: TObject): PPollSocketsSlot; virtual; abstract;
    // extract frames from slot.readbuf, and handle them
    function OnRead(connection: TObject): TPollAsyncSocketOnRead; virtual; abstract;
    // called when slot.writebuf has been sent through the socket
    procedure AfterWrite(connection: TObject); virtual; abstract;
    // pseClosed: should do connection.free - Stop() has been called (socket=0)
    procedure OnClose(aHandle: TAsyncConnectionHandle{;connection: TObject}); virtual; abstract;
    // pseError: return false to close socket and connection (calling OnClose)
    function OnError(connection: TObject; events: TPollSocketEvents): boolean;
      virtual; abstract;
  public
    /// initialize the read/write sockets polling
    // - fRead and fWrite TPollSocketsBuffer instances will track pseRead or
    // pseWrite events, and maintain input and output data buffers
    constructor Create; virtual;
    /// finalize buffer-oriented sockets polling, and release all used memory
    destructor Destroy; override;
    /// assign a new connection to the internal poll
    // - the TSocket handle will be retrieved via SlotFromConnection, and
    // set in non-blocking mode from now on - it is not recommended to access
    // it directly any more, but use Write() and handle OnRead() callback
    // - fRead will poll incoming packets, then call OnRead to handle them,
    // or Unsubscribe and delete the socket when pseClosed is notified
    // - fWrite will poll for outgoing packets as specified by Write(), then
    // send any pending data once the socket is ready
    function Start(connection: TObject): boolean; virtual;
    /// remove a connection from the internal poll, and shutdown its socket
    // - most of the time, the connection is released by OnClose when the other
    // end shutdown the socket; but you can explicitely call this method when
    // the connection (and its socket) is to be shutdown
    // - this method won't call OnClose, since it is initiated by the class
    function Stop(connection: TObject): boolean; virtual;
    /// add some data to the asynchronous output buffer of a given connection
    // - this method may block if the connection is currently writing from
    // another thread (which is not possible from TPollAsyncSockets.Write),
    // up to timeout milliseconds
    function Write(connection: TObject; const data; datalen: integer;
      timeout: integer = 5000): boolean; virtual;
    /// add some data to the asynchronous output buffer of a given connection
    function WriteString(connection: TObject; const data: RawByteString): boolean;
    /// one or several threads should execute this method
    // - thread-safe handle of any incoming packets
    // - if this method is called from a single thread, you should use
    // a TSynThreadPool for any blocking process of OnRead events
    // - otherwise, this method is thread-safe, and incoming packets may be
    // consumed from a set of threads, and call OnRead with newly received data
    procedure ProcessRead(timeoutMS: integer);
    /// one or several threads should execute this method
    // - thread-safe handle of any outgoing packets
    procedure ProcessWrite(timeoutMS: integer);
    /// notify internal socket polls to stop their polling loop ASAP
    procedure Terminate(waitforMS: integer);
    /// low-level access to the polling class used for incoming data
    property PollRead: TPollSockets   read fRead;
    /// low-level access to the polling class used for outgoind data
    property PollWrite: TPollSockets  write fWrite;
  published
    /// how many connections are currently managed by this instance
    property Count: integer  read GetCount;
    /// how many times data has been received by this instance
    property ReadCount: integer  read fReadCount;
    /// how many times data has been sent by this instance
    property WriteCount: integer read fWriteCount;
    /// how many data bytes have been received by this instance
    property ReadBytes: Int64  read fReadBytes;
    /// how many data bytes have been sent by this instance
    property WriteBytes: Int64  read fWriteBytes;
  end;

  {$M-}

function ToText(ev: TPollSocketEvent): PShortString; overload;


{ ******************** Client or Server Asynchronous Process }

type
  /// exception associated with TAsyncConnection / TAsyncConnections process
  EAsyncConnections = class(ESynException);

  /// 32-bit integer value used to identify an asynchronous connection
  // - will start from 1, and increase during the TAsyncConnections live-time

  TAsyncConnections = class;
  TAsyncServer = class;

  TAsyncConnection = class(TSynPersistent)
  protected
    fSlot: TPollSocketsSlot;
    fHandle: TAsyncConnectionHandle;
    fLastOperation: cardinal;
    fRemoteIP: RawUtf8;
    fCanIdleLen:cardinal;
    FLoined:Boolean;
    procedure AfterCreate(Sender: TAsyncConnections); virtual;
    function OnRead(Sender: TAsyncConnections): TPollAsyncSocketOnRead;
      virtual; abstract;
    procedure AfterWrite(Sender: TAsyncConnections); virtual;
    procedure BeforeDestroy(Sender: TAsyncConnections); virtual;
  public
    /// initialize this instance
    constructor Create(const aRemoteIP: RawUtf8); reintroduce; virtual;
    /// read-only access to the socket number associated with this connection
    property Socket: TNetSocket read fSlot.socket;
    property Loined: Boolean read FLoined write FLoined;
  published
    property RemoteIP: RawUtf8 read fRemoteIP;
    property Handle: TAsyncConnectionHandle  read fHandle;
    property CanIdleLen: cardinal  read fCanIdleLen write fCanIdleLen;
  end;

  /// meta-class of one TAsyncConnections connection
  TAsyncConnectionClass = class of TAsyncConnection;
  /// used to store a dynamic array of TAsyncConnection

  TAsyncConnectionObjArray = array of TAsyncConnection;

  /// handle multiple non-blocking connections using TAsyncConnection instances
  // - OnRead will redirect to TAsyncConnection.OnRead virtual method
  // - OnClose will remove the instance from TAsyncConnections.fConnections[]
  // - OnError will return false to shutdown the connection (unless
  // acoOnErrorContinue is defined in TAsyncConnections.Options)
  TAsyncConnectionsSockets = class(TPollAsyncSockets)
  protected
    fOwner: TAsyncConnections;
    function SlotFromConnection(connection: TObject): PPollSocketsSlot; override;
    function OnRead(connection: TObject): TPollAsyncSocketOnRead; override;
    procedure AfterWrite(connection: TObject); override;
    procedure OnClose(aHandle: TAsyncConnectionHandle{;connection: TObject}); override;
    function OnError(connection: TObject; events: TPollSocketEvents): boolean; override;
  public
    /// add some data to the asynchronous output buffer of a given connection
    // - this overriden method will refresh TAsyncConnection.LastOperation
    // - can be executed from an TAsyncConnection.OnRead method
    function Write(connection: TObject; const data; datalen: integer;
      timeout: integer = 5000): boolean; override;
  published

  end;

  /// used to implement a thread poll to process TAsyncConnection instances
  TAsyncConnectionsThread = class(TSynThread)
  protected
    fOwner: TAsyncConnections;
    fProcess: TPollSocketEvent; // pseRead or pseWrite
    procedure Execute; override;
  public
    /// initialize the thread
    constructor Create(aOwner: TAsyncConnections; aProcess: TPollSocketEvent);
      reintroduce;
  end;
  /// low-level options for TAsyncConnections processing
  // - TAsyncConnectionsSockets.OnError will shutdown the connection on any error,
  // unless acoOnErrorContinue is defined
  // - acoOnAcceptFailureStop will let failed Accept() finalize the process
  // - acoNoLogRead and acoNoLogWrite could reduce the log verbosity
  // - acoVerboseLog will log transmitted frames content, for debugging purposes
  // - acoLastOperationNoRead and acoLastOperationNoWrite could be used to
  // avoid TAsyncConnection.fLastOperation reset at read or write
  TAsyncConnectionsOptions = set of (
    acoOnErrorContinue,
    acoOnAcceptFailureStop,
    acoNoLogRead,
    acoNoLogWrite,
    acoVerboseLog,
    acoLastOperationNoRead,
    acoLastOperationNoWrite);

  TServerGeneric = class(TSynThread)
  protected
    fProcessName: RawUtf8;
    fOnHttpThreadStart: TOnNotifyThread;
    procedure SetOnTerminate(const Event: TOnNotifyThread); virtual;
    procedure NotifyThreadStart(Sender: TSynThread);
  public
    /// initialize the server instance, in non suspended state
    constructor Create(CreateSuspended: boolean;
      const OnStart, OnStop: TOnNotifyThread;
      const ProcessName: RawUtf8); reintroduce; virtual;
  end;
  /// implements an abstract thread-pooled high-performance TCP clients or server
  // - internal TAsyncConnectionsSockets will handle high-performance process
  // of a high number of long-living simultaneous connections
  // - will use a TAsyncConnection inherited class to maintain connection state
  // - don't use this abstract class but either TAsyncServer or TAsyncClients
  // - under Linux/POSIX, check your "ulimit -H -n" value: one socket consumes
  // two file descriptors: you may better add the following line to your
  // /etc/limits.conf or /etc/security/limits.conf system file:
  // $ * hard nofile 65535
  TAsyncConnections = class(TServerGeneric)        //            TServerGeneric
  protected
    fBlackBook:TRawUtf8List;
    fStreamClass: TAsyncConnectionClass;
    fConnection: TAsyncConnectionObjArray;
    fConnectionCount: integer;
    fConnections: TDynArray; // fConnection[] sorted by TAsyncConnection.Handle
    fClients: TAsyncConnectionsSockets;
    fThreads: array of TAsyncConnectionsThread;
    fLastHandle: integer;
    fLog: TSynLogClass;
    fTempConnectionForSearchPerHandle: TAsyncConnection;
    fOptions: TAsyncConnectionsOptions;
    fConnectionLock: TSynLocker;

    procedure IdleEverySecond;
    function ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean; virtual;
    function ConnectionAdd(aSocket: TNetSocket; aConnection: TAsyncConnection): boolean; virtual;
    function ConnectionDelete(aHandle: TAsyncConnectionHandle): boolean; overload; virtual;
    function ConnectionDelete(aConnection: TAsyncConnection; aIndex: integer): boolean; overload;
    procedure OnClientClose(connection: TObject); virtual;
    function OnClientConn(ip:RawUtf8):boolean;
  public
    /// initialize the multiple connections
    // - warning: currently reliable only with aThreadPoolCount=1
    constructor Create(const OnStart, OnStop: TOnNotifyThread;
      aStreamClass: TAsyncConnectionClass; const ProcessName: RawUtf8;
      aLog: TSynLogClass; aOptions: TAsyncConnectionsOptions;
      aThreadPoolCount: integer); reintroduce; virtual;
    /// shut down the instance, releasing all associated threads and sockets
    destructor Destroy; override;
    /// high-level access to a connection instance, from its handle
    // - could be executed e.g. from a TAsyncConnection.OnRead method
    // - returns nil if the handle was not found
    // - returns the maching instance, and caller should release the lock as:
    // ! try ... finally UnLock; end;
    function ConnectionFindLocked(aHandle: TAsyncConnectionHandle; aIndex:
      PInteger = nil): TAsyncConnection;
    /// just a wrapper around fConnectionLock.Lock
    procedure Lock;
    /// just a wrapper around fConnectionLock.UnLock
    procedure Unlock;
    /// remove an handle from the internal list, and close its connection
    // - could be executed e.g. from a TAsyncConnection.OnRead method
    function ConnectionRemove(aHandle: TAsyncConnectionHandle): boolean;
    /// add some data to the asynchronous output buffer of a given connection
    // - could be executed e.g. from a TAsyncConnection.OnRead method
    function Write(connection: TAsyncConnection; const data; datalen: integer): boolean; overload;
    /// add some data to the asynchronous output buffer of a given connection
    // - could be executed e.g. from a TAsyncConnection.OnRead method
    function Write(connection: TAsyncConnection; const data: RawByteString): boolean; overload;
    /// log some binary data with proper escape
    // - can be executed from an TAsyncConnection.OnRead method to track content:
    // $ if acoVerboseLog in Sender.Options then Sender.LogVerbose(self,...);
    procedure LogVerbose(connection: TAsyncConnection; const ident: RawUtf8;
      frame: pointer; framelen: integer); overload;
    /// log some binary data with proper escape
    // - can be executed from an TAsyncConnection.OnRead method to track content:
    // $ if acoVerboseLog in Sender.Options then Sender.LogVerbose(...);
    procedure LogVerbose(connection: TAsyncConnection; const ident: RawUtf8;
      const frame: RawByteString); overload;

    procedure AppendBadClient(ip:RawUtf8);
    /// allow to customize low-level options for processing
    property Options: TAsyncConnectionsOptions read fOptions write fOptions;
    /// access to the associated log class
    property Log: TSynLogClass read fLog;
    /// low-level unsafe direct access to the connection instances
    // - ensure this property is used in a thread-safe manner, i.e. via
    // ! Lock; try ... finally UnLock; end;
    property Connection: TAsyncConnectionObjArray read fConnection;
    /// low-level unsafe direct access to the connection count
    // - ensure this property is used in a thread-safe manner, i.e. via
    // ! Lock; try ... finally UnLock; end;
    property ConnectionCount: integer  read fConnectionCount;

    property BlackBook:TRawUtf8List read fBlackBook;
  published
    /// access to the TCP client sockets poll
    // - TAsyncConnection.OnRead should rather use Write() and LogVerbose()
    // methods of this TAsyncConnections class instead of using Clients
//    property Clients: TAsyncConnectionsSockets  read fClients;
    property ProcessName: RawUtf8 read fProcessName write fProcessName;
  end;

  TAsyncServer = class(TAsyncConnections)
  protected
    fServer: TCrtSocket;
    fExecuteFinished: boolean;
    procedure Execute; override;
  public
    /// run the TCP server, listening on a supplied IP port
    constructor Create(const aPort: RawUtf8;
      const OnStart, OnStop: TOnNotifyThread;
      aStreamClass: TAsyncConnectionClass; const ProcessName: RawUtf8;
      aLog: TSynLogClass; aOptions: TAsyncConnectionsOptions;
      aThreadPoolCount: integer = 1); reintroduce; virtual;
    destructor Destroy; override;
  published
    /// access to the TCP server socket
    property ServerSocket: TCrtSocket read fServer;
  end;

implementation

{ ******************** Low-Level Non-blocking Connections }

function ToText(ev: TPollSocketEvent): PShortString;
begin
  result := GetEnumName(TypeInfo(TPollSocketEvent), ord(ev));
end;

{ TPollSocketsSlot }

function TPollSocketsSlot.Lock(writer: boolean): boolean;
begin
  result := InterlockedIncrement(lockcounter[writer]) = 1;
  if not result then
    LockedDec32(@lockcounter[writer]);
end;

procedure TPollSocketsSlot.Unlock(writer: boolean);
begin
  if @self <> nil then
    LockedDec32(@lockcounter[writer]);
end;

function TPollSocketsSlot.TryLock(writer: boolean; timeoutMS: cardinal): boolean;
var
  endtix: Int64;
  ms: integer;
begin
  result := (@self <> nil) and
            (socket <> nil);
  if not result then
    exit; // socket closed
  result := Lock(writer);
  if result or
     (timeoutMS = 0) then
    exit; // we acquired the slot, or we don't want to wait
  endtix := GetTickCount64 + timeoutMS; // never wait forever
  ms := 0;
  repeat
    SleepHiRes(ms);
    ms := ms xor 1; // 0,1,0,1,0,1...
    if socket = nil then
      exit; // no socket to lock for
    result := Lock(writer);
    if result then
    begin
      result := socket <> nil;
      if not result then
        UnLock(writer);
      exit; // acquired or socket closed
    end;
  until GetTickCount64 >= endtix;
end;


{ TPollAsyncSockets }

constructor TPollAsyncSockets.Create;
var
  c: TPollSocketClass;
begin
  inherited Create;
  fWriteDirect := true;
  c := PollSocketClass;
  fRead := TPollSockets.Create(c);
  { TODO : try TPollSocketEPoll for fWrite on LINUXNOTBSD ? }
  fWrite := TPollSockets.Create(c);
end;

destructor TPollAsyncSockets.Destroy;
begin
  if not fRead.Terminated then
    Terminate(5000);
  inherited Destroy;
  fRead.Free;
  fWrite.Free;
end;

function TPollAsyncSockets.Start(connection: TObject): boolean;
var
  slot: PPollSocketsSlot;
begin
  result := false;
  if (fRead.Terminated) or
     (connection = nil) then
    exit;
  LockedInc32(@fProcessing);
  try
    slot := SlotFromConnection(connection);
    if (slot = nil) or
       (slot.socket = nil) then
      exit;
    if slot.socket.MakeAsync <> nrOK then
      exit; // we expect non-blocking mode on a real working socket
    result := fRead.Subscribe(slot.socket, [pseRead], TPollSocketTag(connection));
    // now, ProcessRead will handle pseRead + pseError/pseClosed on this socket
  finally
    LockedDec32(@fProcessing);
  end;
end;

function TPollAsyncSockets.Stop(connection: TObject): boolean;
var
  slot: PPollSocketsSlot;
  sock: TNetSocket;
  endtix: Int64;
  lock: set of (r, w);
  dummy: byte;
  dummylen: integer;
begin
  result := false;
  if fRead.Terminated or
     (connection = nil) then
    exit;
  LockedInc32(@fProcessing);
  try
    slot := SlotFromConnection(connection);
    if slot = nil then
      exit;
    sock := slot.socket;
    if sock <> nil then
    try
      slot.socket := nil; // notify ProcessRead/ProcessWrite to abort
      dummylen := SizeOf(dummy);
      slot.lastWSAError := sock.Recv(@dummy, dummylen);
      if slot.lastWSAError = nrClosed then
        slot.lastWSAError := nrOK;
      fRead.Unsubscribe(sock, TPollSocketTag(connection));
      fWrite.Unsubscribe(sock, TPollSocketTag(connection));
      result := true;
    finally
      sock.ShutdownAndClose({rdwr=}false);
      endtix := GetTickCount64 + 10000;
      lock := [];
      repeat // acquire locks to avoid OnClose -> Connection.Free -> GPF
        if not (r in lock) and slot.Lock(false) then
          include(lock, r);
        if not (w in lock) and slot.Lock(true) then
          include(lock, w);
        if lock = [r, w] then
          break;
        SleepHiRes(0); // 10 microsecs on POSIX
      until GetTickCount64 >= endtix;
    end;
  finally
    LockedDec32(@fProcessing);
  end;
end;

function TPollAsyncSockets.GetCount: integer;
begin
  if self = nil then
    result := 0
  else
    result := fRead.Count;
end;

procedure TPollAsyncSockets.Terminate(waitforMS: integer);
var
  endtix: Int64;
begin
  fRead.Terminate;
  fWrite.Terminate;
  if waitforMS <= 0 then
    exit;
  endtix := GetTickCount64 + waitforMS;
  repeat
    SleepHiRes(1);
    if fProcessing = 0 then
      break;
  until GetTickCount64 > endtix;
end;

function TPollAsyncSockets.WriteString(connection: TObject;
  const data: RawByteString): boolean;
begin
  if self = nil then
    result := false
  else
    result := Write(connection, pointer(data)^, length(data));
end;

procedure AppendData(var buf: RawByteString; const data; datalen: PtrInt);
var
  buflen: PtrInt;
begin
  if datalen > 0 then
  begin
    buflen := length(buf);
    SetLength(buf, buflen + datalen);
    MoveFast(data, PByteArray(buf)^[buflen], datalen);
  end;
end;

function TPollAsyncSockets.Write(connection: TObject; const data;
  datalen, timeout: integer): boolean;
var
  tag: TPollSocketTag;
  slot: PPollSocketsSlot;
  P: PByte;
  res: TNetResult;
  sent, previous: integer;
begin
  result := false;
  if (datalen <= 0) or
     (connection = nil) or
     fWrite.Terminated then
    exit;
  LockedInc32(@fProcessing);
  try
    tag := TPollSocketTag(connection);
    slot := SlotFromConnection(connection);
    if (slot = nil) or
       (slot.socket = nil) then
      exit;
    if slot.TryLock(true, timeout) then // try and wait for another ProcessWrite
    try
      P := @data;
      previous := length(slot.writebuf);
      if (previous = 0) and fWriteDirect then
        repeat
          // try to send now in non-blocking mode (works most of the time)
          if fWrite.Terminated or
             (slot.socket = nil) then
            exit;
          sent := datalen;
          res := slot.socket.Send(P, sent);
          if slot.socket = nil then
            exit;  // Stop() called
          if res = nrRetry then
            break; // fails now -> retry later in ProcessWrite
          if res <> nrOK then
            exit;  // connection closed or broken -> abort
          inc(fWriteCount);
          inc(fWriteBytes, sent);
          dec(datalen, sent);
          if datalen = 0 then
          begin
            try
              // notify everything written
              AfterWrite(connection);
              result := true;
            except
              result := false;
            end;
            exit;
          end;
          inc(P, sent);
        until false;
        // use fWrite output polling for the remaining data in ProcessWrite
      AppendData(slot.writebuf, P^, datalen);
      if previous > 0 then // already subscribed
        result := slot.socket <> nil
      else if fWrite.Subscribe(slot.socket, [pseWrite], tag) then
        result := slot.socket <> nil
      else
        slot.writebuf := ''; // subscription error -> abort
    finally
      slot.UnLock({writer=}true);
    end;
  finally
    LockedDec32(@fProcessing);
  end;
end;

procedure TPollAsyncSockets.ProcessRead(timeoutMS: integer);
var
  notif: TPollSocketResult;
  connection: TObject;
  slot: PPollSocketsSlot;
  recved, added: integer;
  res: TNetResult;
  temp: array[0..2048] of byte; // read up to 32KB per chunk

  procedure CloseConnection(withinreadlock: boolean);
  begin
    if withinreadlock then
      slot.UnLock({writer=}false); // Stop() will try to acquire this lock

    //Stop(connection); // shutdown and set socket:=0 + acquire locks
    try
      OnClose((connection as TAsyncConnection).Handle); // now safe to perform connection.Free
    except
      connection := nil;   // user code may be unstable
    end;
    slot := nil; // ignore pseClosed and slot.Unlock(false)
  end;

begin
  if (self = nil) or
     fRead.Terminated then
    exit;
  LockedInc32(@fProcessing);
  try
    if not fRead.GetOne(timeoutMS, notif) then
      exit;
    connection := TObject(notif.tag);
    slot := SlotFromConnection(connection);
    if (slot = nil) or
       (slot.socket = nil) then
      exit;
    if pseError in notif.events then
      if not OnError(connection, notif.events) then
      begin
        // false = shutdown
        CloseConnection({withinlock=}false);
        exit;
      end;
    if pseRead in notif.events then
    begin
      if slot.Lock({writer=}false) then // paranoid thread-safe read
      try
        added := 0;
        repeat
          if fRead.Terminated or
             (slot.socket = nil) then
            exit;
          recved := SizeOf(temp);
          res := slot.socket.Recv(@temp, recved);
          if slot.socket = nil then
            exit; // Stop() called
          if res = nrRetry then
            break; // may block, try later
          if res <> nrOk then
          begin
            CloseConnection(true);
            exit; // socket closed gracefully or unrecoverable error -> abort
          end;
          AppendData(slot.readbuf, temp, recved);
          inc(added, recved);
        until false;
        if added > 0 then
        try
          inc(fReadCount);
          inc(fReadBytes, added);
          if OnRead(connection) = sorClose then
            CloseConnection(true);
        except
          CloseConnection(true); // force socket shutdown
        end;
      finally
        slot.UnLock(false); // CloseConnection may set slot=nil
      end;
    end;
    if (slot <> nil) and
       (slot.socket <> nil) and
       (pseClosed in notif.events) then
    begin
      CloseConnection(false);
      exit;
    end;
  finally
    LockedDec32(@fProcessing);
  end;
end;

procedure TPollAsyncSockets.ProcessWrite(timeoutMS: integer);
var
  notif: TPollSocketResult;
  connection: TObject;
  slot: PPollSocketsSlot;
  buf: PByte;
  buflen, bufsent, sent: integer;
  res: TNetResult;
begin
  if (self = nil) or
     fWrite.Terminated then
    exit;
  LockedInc32(@fProcessing);
  try
    if not fWrite.GetOne(timeoutMS, notif) then
      exit;
    if notif.events <> [pseWrite] then
      exit; // only try if we are sure the socket is writable and safe
    connection := TObject(notif.tag);
    slot := SlotFromConnection(connection);
    if (slot = nil) or
       (slot.socket = nil) then
      exit;
    if slot.Lock({writer=}true) then // paranoid check
    try
      buflen := length(slot.writebuf);
      if buflen <> 0 then
      begin
        buf := pointer(slot.writebuf);
        sent := 0;
        repeat
          if fWrite.Terminated or
             (slot.socket = nil) then
            exit;
          bufsent := buflen;
          res := slot.socket.Send(buf, bufsent);
          if slot.socket = nil then
            exit; // Stop() called
          if res = nrRetry then
            break; // may block, try later
          if res <> nrOk then
            exit; // socket closed gracefully or unrecoverable error -> abort
          inc(fWriteCount);
          inc(sent, bufsent);
          inc(buf, bufsent);
          dec(buflen, bufsent);
        until buflen = 0;
        inc(fWriteBytes, sent);
        delete(slot.writebuf, 1, sent);
      end;
      if slot.writebuf = '' then
      begin
        // no data any more to be sent
        fWrite.Unsubscribe(slot.socket, notif.tag);
        try
          AfterWrite(connection);
        except
        end;
      end;
    finally
      slot.UnLock(true);
    end;
  finally
    LockedDec32(@fProcessing);
  end;
end;


{ ******************** Client or Server Asynchronous Process }

{ TAsyncConnection }

constructor TAsyncConnection.Create(const aRemoteIP: RawUtf8);
begin
  inherited Create;
  fRemoteIP := aRemoteIP;
  FLoined := false;
end;

procedure TAsyncConnection.AfterCreate(Sender: TAsyncConnections);
begin
  fLastOperation := UnixTimeUtc;
end;

procedure TAsyncConnection.AfterWrite(Sender: TAsyncConnections);
begin
  fLastOperation := UnixTimeUtc;
end;

procedure TAsyncConnection.BeforeDestroy(Sender: TAsyncConnections);
begin
  fHandle := 0; // to detect any dangling pointer
end;


{ TAsyncConnectionsSockets }

procedure TAsyncConnectionsSockets.OnClose(aHandle: TAsyncConnectionHandle{;connection: TObject});
begin
//  // caller did call Stop() before calling OnClose (socket=0)
//  if connection=nil then exit;      //add by me
//  fOwner.fLog.Add.Log(sllTrace, 'OnClose%', [connection], self);
//  fOwner.ConnectionDelete((connection as TAsyncConnection).Handle); // do connection.Free
//  fOwner.OnClientClose(connection);
   fowner.ConnectionRemove(aHandle);
end;

function TAsyncConnectionsSockets.OnError(connection: TObject; events:
  TPollSocketEvents): boolean;
begin
  fOwner.fLog.Add.Log(sllDebug,
    'OnError% events=[%] -> free socket and instance', [connection,
    GetSetName(TypeInfo(TPollSocketEvents), events)], self);
  result := false;// acoOnErrorContinue in fOwner.Options; // false=close by default
end;

function TAsyncConnectionsSockets.OnRead(connection: TObject): TPollAsyncSocketOnRead;
var
  ac: TAsyncConnection;
begin
  ac := connection as TAsyncConnection;
  if not (acoNoLogRead in fOwner.Options) then
    fOwner.fLog.Add.Log(sllTrace, 'OnRead% len=%', [ac, length(ac.fSlot.readbuf)], self);
  result := ac.OnRead(fOwner);
  if not (acoLastOperationNoRead in fOwner.Options) then
    ac.fLastOperation := UnixTimeUtc;
end;

function TAsyncConnectionsSockets.SlotFromConnection(connection: TObject):
  PPollSocketsSlot;
begin
  try
    if (connection = nil) or
       not connection.InheritsFrom(TAsyncConnection) or
       (TAsyncConnection(connection).Handle = 0) then
    begin
      fOwner.fLog.Add.Log(sllStackTrace,
        'SlotFromConnection() with dangling pointer %', [connection], self);
      result := nil;
    end
    else
      result := @TAsyncConnection(connection).fSlot;
  except
    fOwner.fLog.Add.Log(sllError, 'SlotFromConnection() with dangling pointer %',
      [pointer(connection)], self);
    result := nil;
  end;
end;

function TAsyncConnectionsSockets.Write(connection: TObject; const data;
  datalen, timeout: integer): boolean;
var
  tmp: TLogEscape;
begin
  result := inherited Write(connection, data, datalen, timeout);
  if result and
     not (acoLastOperationNoWrite in fOwner.Options) then
    (connection as TAsyncConnection).fLastOperation := UnixTimeUtc;
  if (fOwner.fLog <> nil) and
     not (acoNoLogWrite in fOwner.Options) then
    fOwner.fLog.Add.Log(sllTrace, 'Write%=% len=%%', [connection,
      BOOL_STR[result], datalen, LogEscape(@data, datalen, tmp{%H-},
      acoVerboseLog in fOwner.Options)], self);
end;

procedure TAsyncConnectionsSockets.AfterWrite(connection: TObject);
begin
  (connection as TAsyncConnection).AfterWrite(fOwner);
end;

{ TAsyncConnectionsThread }

constructor TAsyncConnectionsThread.Create(aOwner: TAsyncConnections; aProcess:
  TPollSocketEvent);
begin
  fOwner := aOwner;
  fProcess := aProcess;
  fOnThreadTerminate := fOwner.fOnThreadTerminate;
  inherited Create(false);
end;

procedure TAsyncConnectionsThread.Execute;
//var
//  idletix: Int64;
begin
  SetCurrentThreadName('% % %', [fOwner.fProcessName, self, ToText(fProcess)^]);
  fOwner.NotifyThreadStart(self);
  try
//    idletix := mormot.core.os.GetTickCount64 + 1000;
    while not Terminated and
          (fOwner.fClients <> nil) do
    begin
        case fProcess of
          pseRead:
            begin
              fOwner.fClients.ProcessRead(30000);
//              if (mormot.core.os.GetTickCount64 >= idletix) then
//              begin
//                fOwner.IdleEverySecond; // may take some time -> retrieve ticks again
//                idletix := mormot.core.os.GetTickCount64 + 1000;
//              end;
            end;
          pseWrite:
            fOwner.fClients.ProcessWrite(30000);
          pseError:
            begin
              fOwner.IdleEverySecond; // may take some time -> retrieve ticks again
              sleep(1000);
            end;
        else
          raise EAsyncConnections.CreateUtf8('%.Execute: unexpected fProcess=%',
            [self, ToText(fProcess)^]);
        end;
    end;
  except
    on E: Exception do
      fOwner.fLog.Add.Log(sllWarning, 'Execute raised a % -> terminate % thread',
        [E.ClassType, fOwner.fStreamClass], self);
  end;
  fOwner.fLog.Add.Log(sllDebug, 'Execute: done', self);
end;


{ TAsyncConnections }

function TAsyncConnectionCompareByHandle(const A, B): integer;
begin
  // for fast binary search from the connection handle
  result := TAsyncConnection(A).Handle - TAsyncConnection(B).Handle;
end;

constructor TAsyncConnections.Create(const OnStart, OnStop: TOnNotifyThread;
  aStreamClass: TAsyncConnectionClass; const ProcessName: RawUtf8;
  aLog: TSynLogClass; aOptions: TAsyncConnectionsOptions; aThreadPoolCount: integer);
var
  i: PtrInt;
  log: ISynLog;
begin
  log := aLog.Enter('Create(%,%,%)', [aStreamClass, ProcessName, aThreadPoolCount], self);
  if (aStreamClass = TAsyncConnection) or
     (aStreamClass = nil) then
    raise EAsyncConnections.CreateUtf8('%.Create(%)', [self, aStreamClass]);
  if aThreadPoolCount <= 0 then
    aThreadPoolCount := 1;

  fBlackBook := TRawUtf8List.Create([]);

  fLog := aLog;
  fStreamClass := aStreamClass;
  fConnectionLock.Init;
  fConnections.Init(TypeInfo(TPointerDynArray), fConnection, @fConnectionCount);
  // don't use TAsyncConnectionObjArray to manually call TAsyncConnection.BeforeDestroy
  fConnections.Compare := TAsyncConnectionCompareByHandle;
  fClients := TAsyncConnectionsSockets.Create;
  fClients.fOwner := self;
  fTempConnectionForSearchPerHandle := fStreamClass.Create('');
  fOptions := aOptions;
  inherited Create(false, OnStart, OnStop, ProcessName);

  SetLength(fThreads, aThreadPoolCount + 2);
  fThreads[0] := TAsyncConnectionsThread.Create(self, pseWrite);
  fThreads[1] := TAsyncConnectionsThread.Create(self,pseError);
  for i := 1 to aThreadPoolCount do
    fThreads[i+1] := TAsyncConnectionsThread.Create(self, pseRead);
end;

destructor TAsyncConnections.Destroy;
var
  i: PtrInt;
begin
  if fClients <> nil then
    with fClients do
      fLog.Add.Log(sllDebug, 'Destroy total=% reads=%/% writes=%/%',
        [fLastHandle, ReadCount, KB(ReadBytes), WriteCount, KB(WriteBytes)], self);
  Terminate;
  for i := 0 to high(fThreads) do
    fThreads[i].Terminate; // stop ProcessRead/ProcessWrite when polling stops
  FreeAndNil(fClients); // stop polling and refuse further Write/ConnectionRemove
  ObjArrayClear(fThreads);
  inherited Destroy;
  for i := 0 to fConnectionCount - 1 do
  try
    fConnection[i].BeforeDestroy(self);
    fConnection[i].Free;
  except
  end;
  fConnectionLock.Done;
  fTempConnectionForSearchPerHandle.Free;
  fBlackBook.Free;
end;

function TAsyncConnections.ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean;
begin
  result := false;
end;

function TAsyncConnections.ConnectionAdd(aSocket: TNetSocket;
  aConnection: TAsyncConnection): boolean;
begin
  result := false; // caller should release aSocket
  if Terminated then
    exit;
  aConnection.fSlot.socket := aSocket;
  fConnectionLock.Lock;
  try
    inc(fLastHandle);
    aConnection.fHandle := fLastHandle;
    fConnections.Add(aConnection);
    fLog.Add.Log(sllTrace, 'ConnectionAdd% count=%',
      [aConnection, fConnectionCount], self);
    fConnections.Sorted := true; // handles are increasing
  finally
    fConnectionLock.UnLock;
  end;
  aConnection.AfterCreate(self);
  result := true; // indicates aSocket owned by the pool
end;

function TAsyncConnections.ConnectionDelete(aConnection: TAsyncConnection;
  aIndex: integer): boolean;
var
  t: TClass;
  h: TAsyncConnectionHandle;
begin
  // caller should have done fConnectionLock.Lock
  try
    h := aConnection.Handle;
    t := aConnection.ClassType;
    aConnection.BeforeDestroy(self);
    aConnection.Free;
  finally
    fConnections.FastDeleteSorted(aIndex);
  end;
  fLog.Add.Log(sllTrace, 'ConnectionDelete %.Handle=% count=%',
    [t, h, fConnectionCount], self);
  result := true;
end;

function TAsyncConnections.ConnectionDelete(aHandle: TAsyncConnectionHandle): boolean;
var
  i: integer;
  conn: TAsyncConnection;
begin
  // don't call fClients.Stop() here - see ConnectionRemove()
  result := false;
  if Terminated or
     (aHandle <= 0) then
    exit;
  conn := ConnectionFindLocked(aHandle, @i);
  if conn <> nil then
  try
    result := ConnectionDelete(conn, i);
  finally
    fConnectionLock.UnLock;
  end;
  if not result then
    fLog.Add.Log(sllTrace, 'ConnectionDelete(%)=false count=%',
      [aHandle, fConnectionCount], self);
end;

function TAsyncConnections.ConnectionFindLocked(aHandle: TAsyncConnectionHandle;
  aIndex: PInteger): TAsyncConnection;
var
  i: integer;
begin
  result := nil;
  if (self = nil) or
     Terminated or
     (aHandle <= 0) then
    exit;
  fConnectionLock.Lock;
  try
    fTempConnectionForSearchPerHandle.fHandle := aHandle;
    // fast O(log(n)) binary search
    i := fConnections.Find(fTempConnectionForSearchPerHandle);
    if i >= 0 then
    begin
      result := fConnection[i];
      if aIndex <> nil then
        aIndex^ := i;
    end;
    fLog.Add.Log(sllTrace, 'ConnectionFindLocked(%)=%', [aHandle, result], self);
  finally
    if result = nil then
      fConnectionLock.UnLock;
  end;
end;

function TAsyncConnections.ConnectionRemove(aHandle: TAsyncConnectionHandle): boolean;
var
  i: integer;
  conn: TAsyncConnection;
begin
  result := false;
  if (self = nil) or
     Terminated or
     (aHandle <= 0) then
    exit;
  conn := ConnectionFindLocked(aHandle, @i);
  if conn <> nil then
  try
//    conn.onclose
    if not fClients.Stop(conn) then
      fLog.Add.Log(sllDebug, 'ConnectionRemove: Stop=false for %', [conn], self);
    result := ConnectionDelete(conn, i);
  finally
    fConnectionLock.UnLock;
  end;
  if not result then
    fLog.Add.Log(sllTrace, 'ConnectionRemove(%)=false', [aHandle], self);
end;

procedure TAsyncConnections.lock;
begin
  fConnectionLock.Lock;
end;

procedure TAsyncConnections.Unlock;
begin
  fConnectionLock.UnLock;
end;

function TAsyncConnections.Write(connection: TAsyncConnection; const data;
  datalen: integer): boolean;
begin
  if Terminated then
    result := false
  else
    result := fClients.Write(connection, data, datalen);
end;

function TAsyncConnections.Write(connection: TAsyncConnection;
  const data: RawByteString): boolean;
begin
  if Terminated then
    result := false
  else
    result := fClients.WriteString(connection, data);
end;

procedure TAsyncConnections.LogVerbose(connection: TAsyncConnection;
  const ident: RawUtf8; frame: pointer; framelen: integer);
var
  tmp: TLogEscape;
begin
  if not (acoNoLogRead in Options) and
     (acoVerboseLog in Options) and
     (fLog <> nil) then
    fLog.Add.Log(sllTrace, '% len=%%', [ident, framelen, LogEscape(frame,
      framelen, tmp{%H-})], connection);
end;

procedure TAsyncConnections.LogVerbose(connection: TAsyncConnection;
  const ident: RawUtf8; const frame: RawByteString);
begin
  LogVerbose(connection, ident, pointer(frame), length(frame));
end;

procedure TAsyncConnections.AppendBadClient(ip: RawUtf8);
var
  i:integer;
  blackobj:TBlackObj;
begin
  fBlackBook.Safe.Lock;
  try
    for i := fBlackBook.Count - 1 downto 0 do
    if fBlackBook[i] = ip then
    begin
      blackobj := fBlackBook.ObjectPtr[i];
      inc(blackobj.count);
      blackobj.lasttime := UnixTimeUtc;
      exit;
    end;
    blackobj := TBlackObj.Create;
    inc(blackobj.count);
    blackobj.lasttime := UnixTimeUtc;
    fBlackBook.AddObject(ip,blackobj);
  finally
    fBlackBook.Safe.UnLock;
  end;
end;

procedure TAsyncConnections.OnClientClose(connection: TObject);
begin

end;

function TAsyncConnections.OnClientConn(ip: RawUtf8):boolean;
var
  i:integer;
  blackobj:TBlackObj;
begin
  result := false;
  fBlackBook.Safe.Lock;
  try
    for i := fBlackBook.Count - 1 downto 0 do
    if fBlackBook[i] = ip then
    begin
      blackobj := fBlackBook.ObjectPtr[i];
      if blackobj.count>15 then
      begin
        if (UnixTimeUtc - blackobj.lasttime)>15*60 then
        begin
          blackobj.count := 0;
          break;
        end else exit;
      end else
        break;
    end;
  finally
    fBlackBook.Safe.UnLock;
  end;
  result := true;
end;

procedure TAsyncConnections.IdleEverySecond;
var
  i: integer;
  allowed: cardinal;
  aconn: TAsyncConnection;
  IdleArr: TDynArray;
  IntegerArr: TIntegerDynArray;
begin
  if Terminated then
    exit;
  IdleArr.Init(TypeInfo(TIntegerDynArray),IntegerArr);
  fConnectionLock.Lock;
  try
    for i:=0 to fConnectionCount-1 do
    begin
      aconn := fConnection[i];
      allowed := UnixTimeUtc - aconn.fCanIdleLen;
      if aconn.fLastOperation < allowed then
      begin
        if (not aconn.FLoined) and (aconn.fSlot.readbuf='') then
           AppendBadClient(aconn.fRemoteIP);
        IdleArr.Add(aconn.handle);
        //if not fClients.Stop(aconn) then
        //  fLog.Add.Log(sllDebug, 'ConnectionRemove: Stop=false for %', [aconn], self);
//        ConnectionDelete(aconn,i);
      end;
      if Terminated then  exit;
    end;
  finally
    fConnectionLock.UnLock;
  end;
  //
  for i:=0 to High(IntegerArr) do
    ConnectionRemove(IntegerArr[i]);
end;


{ TAsyncServer }

constructor TAsyncServer.Create(const aPort: RawUtf8;
  const OnStart, OnStop: TOnNotifyThread; aStreamClass: TAsyncConnectionClass;
  const ProcessName: RawUtf8; aLog: TSynLogClass;
  aOptions: TAsyncConnectionsOptions; aThreadPoolCount: integer);
begin
  RemoteIPLocalHostAsVoidInServers := false;
  fServer := TCrtSocket.Bind(aPort);
  inherited Create(OnStart, OnStop, aStreamClass, ProcessName, aLog,
    aOptions, aThreadPoolCount);
end;

destructor TAsyncServer.Destroy;
var
  endtix: Int64;
  touchandgo: TNetSocket; // paranoid ensure Accept() is released
begin
  Terminate;
  if fServer <> nil then
  begin
    fServer.Close; // shutdown the socket to unlock Accept() in Execute
    if NewSocket('127.0.0.1', fServer.Port, nlTCP, false, 1000, 0, 0, 0, touchandgo) = nrOk then
      touchandgo.ShutdownAndClose(false);
  end;
  endtix := mormot.core.os.GetTickCount64 + 10000;
  inherited Destroy;
  while not fExecuteFinished and
        (mormot.core.os.GetTickCount64 < endtix) do
    SleepHiRes(1); // wait for Execute to be finalized (unlikely)
  fServer.Free;
end;

procedure TAsyncServer.Execute;
var
  client: TNetSocket;
  connection: TAsyncConnection;
  res: TNetResult;
  sin: TNetAddr;
  ip: RawUtf8;
begin
  SetCurrentThreadName('% % Accept', [fProcessName, self]);
  NotifyThreadStart(self);
  if fServer.Sock <> nil then
  try
    while not Terminated do
    begin
      res := fServer.Sock.Accept(client, sin);
      if res <> nrOk then
        if Terminated then
          break
        else
        begin
          fLog.Add.Log(sllWarning, 'Execute: Accept()=%', [ToText(res)^], self);
          SleepHiRes(1);
          continue;
        end;
      if Terminated then
      begin
        client.ShutdownAndClose({rdwr=}false);
        break;
      end;
      ip := sin.IP;
      if not OnClientConn(ip) then
      begin
        client.ShutdownAndClose(false);
        continue;
      end;

      if ConnectionCreate(client, ip, connection) then
        if fClients.Start(connection) then
          fLog.Add.Log(sllTrace, 'Execute: Accept()=%', [connection], self)
        else
          connection.Free   //some err;
      else
        client.ShutdownAndClose(false);
    end;
  except
    on E: Exception do
      fLog.Add.Log(sllWarning, 'Execute raised % -> terminate %',
        [E.ClassType, fProcessName], self);
  end;
  fLog.Add.Log(sllDebug, 'Execute: % done', [fProcessName], self);
  fExecuteFinished := true;
end;

{ TServerGeneric }

constructor TServerGeneric.Create(CreateSuspended: boolean; const OnStart,
  OnStop: TOnNotifyThread; const ProcessName: RawUtf8);
begin
  fProcessName := ProcessName;
  fOnHttpThreadStart := OnStart;
  SetOnTerminate(OnStop);
  inherited Create(CreateSuspended);
end;

procedure TServerGeneric.NotifyThreadStart(Sender: TSynThread);
begin
  if Sender = nil then
    raise EAsyncConnections.CreateUtf8('%.NotifyThreadStart(nil)', [self]);
  if Assigned(fOnHttpThreadStart) and
     not Assigned(Sender.StartNotified) then
  begin
    fOnHttpThreadStart(Sender);
    Sender.StartNotified := self;
  end;
end;

procedure TServerGeneric.SetOnTerminate(const Event: TOnNotifyThread);
begin
  fOnThreadTerminate := Event;
end;

end.


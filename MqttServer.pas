unit MqttServer;

interface

{$I ..\mormot.defines.inc}

uses
  sysutils,
  classes,
  mormot.core.base,
  mormot.core.os,
  mormot.core.data,  
  mormot.core.threads,
  mormot.core.unicode,
  mormot.core.text,
  mormot.net.sock;

const
  // kept-alive or big HTTP requests will create a dedicated THttpServerResp
  // - each thread reserves 2 MB of memory so it may break the server
  // - keep the value to a decent number, to let resources be constrained up to 1GB
  // - is the default value to TSynThreadPoolTHttpServer.MaxBodyThreadCount
  THREADPOOL_MAXWORKTHREADS = 512;

  /// if HTTP body length is bigger than 16 MB, creates a dedicated THttpServerResp
  // - is the default value to TSynThreadPoolTHttpServer.BigBodySize
  THREADPOOL_BIGBODYSIZE = 16 * 1024 * 1024;

type
TMqttServerConnectionID = Int64;

  EMqttServer = class(ESynException);

  {$M+} // to have existing RTTI for published properties
  TMqttServerGeneric = class;
  {$M-}

  TMqttServerRequest = class
  protected
    fRemoteIP:RawUtf8;
    fOutContent:RawUtf8;
    fUseSSL: boolean;
    fServer: TMqttServerGeneric;
    fConnectionThread: TSynThread;
    fConnectionID: TMqttServerConnectionID;
  public
    /// initialize the context, associated to a HTTP server instance
    constructor Create(aServer: TMqttServerGeneric;
      aConnectionID: TMqttServerConnectionID; aConnectionThread: TSynThread); virtual;
    /// prepare an incoming request
    // - will set input parameters URL/Method/InHeaders/InContent/InContentType
    // - will reset output parameters
    procedure Prepare(const aURL, aMethod, aInHeaders: RawUtf8;
      const aInContent: RawByteString; const aInContentType, aRemoteIP: RawUtf8;
      aUseSSL: boolean = false);
    property OutContent: RawUtf8 read fOutContent;
          
    property Server: TMqttServerGeneric read fServer;
    property ConnectionThread: TSynThread read fConnectionThread;
  end;

  TMqttGeneric = class(TSynThread)
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

  /// abstract class to implement a HTTP server
  // - do not use this class, but rather the THttpServer or THttpApiServer
  TMqttServerGeneric = class(TMqttGeneric)
  private
    fOnThreadTerminate: TOnNotifyThread;
  protected
    fShutdownInProgress: boolean;
    fMaximumAllowedContentLength: cardinal;
    fCurrentConnectionID: integer; // 31-bit NextConnectionID sequence
    fCurrentRequestID: integer;
    fCanNotifyCallback: boolean;
    procedure SetMaximumAllowedContentLength(aMax: cardinal); virtual;
    function GetHttpQueueLength: cardinal; virtual; abstract;
    procedure SetHttpQueueLength(aValue: cardinal); virtual; abstract;
    function NextConnectionID: integer; // 31-bit internal sequence
  public
    constructor Create(CreateSuspended: boolean;
      const OnStart, OnStop: TOnNotifyThread;
      const ProcessName: RawUtf8); reintroduce; virtual;
    function Callback(Ctxt: TMqttServerRequest; aNonBlocking: boolean): cardinal; virtual;
    /// you can call this method to prepare the HTTP server for shutting down
    procedure Shutdown;
    property OnHttpThreadStart: TOnNotifyThread  read fOnHttpThreadStart write fOnHttpThreadStart;
    property OnHttpThreadTerminate: TOnNotifyThread read fOnThreadTerminate write SetOnTerminate;
    property MaximumAllowedContentLength: cardinal  read fMaximumAllowedContentLength write SetMaximumAllowedContentLength;
    property HttpQueueLength: cardinal read GetHttpQueueLength write SetHttpQueueLength;
    /// TRUE if the inherited class is able to handle callbacks
    // - only TWebSocketServer has this ability by now
    property CanNotifyCallback: boolean  read fCanNotifyCallback;
  published
    /// the associated process name
    property ProcessName: RawUtf8 read fProcessName write fProcessName;
  end;

  THttpServerSocketGetRequestResult = (
    grError,
    grException,
    grOversizedPayload,
    grRejected,
    grTimeout,
    grHeaderReceived,
    grBodyReceived,
    grOwned);

  {$M+} // to have existing RTTI for published properties
  TMqttServer = class;
  {$M-}

  TMqttServerSocket = class(TCrtSocket)
  protected
    fContent: RawUtf8;
    fContentLength: Integer;
    fKeepAliveClient: boolean;
    fRemoteConnectionID: TMqttServerConnectionID;
    fServer: TMqttServer;
  public
    constructor Create(aServer: TMqttServer); reintroduce;
    function GetRequest(withBody: boolean;
      headerMaxTix: Int64): THttpServerSocketGetRequestResult; virtual;
    property Content: RawUtf8  read fContent;
    property ContentLength: Integer  read fContentLength;
    property KeepAliveClient: boolean  read fKeepAliveClient write fKeepAliveClient;
    property RemoteConnectionID: TMqttServerConnectionID read fRemoteConnectionID;
  end;

  TMqttServerSocketClass = class of TMqttServerSocket;

  TMqttServerResp = class(TSynThread)
  protected
    fServer: TMqttServer;
    fServerSock: TMqttServerSocket;
    fClientSock: TNetSocket;
    fClientSin: TNetAddr;
    fConnectionID: TMqttServerConnectionID;
    procedure Execute; override;
  public
    constructor Create(aSock: TNetSocket; const aSin: TNetAddr;
      aServer: TMqttServer); reintroduce; overload;
    constructor Create(aServerSock: TMqttServerSocket; aServer: TMqttServer);
      reintroduce; overload; virtual;
    property ServerSock: TMqttServerSocket read fServerSock;
    property Server: TMqttServer  read fServer;
    property ConnectionID: TMqttServerConnectionID read fConnectionID;
  end;
  TMqttServerRespClass = class of TMqttServerResp;
  
  TSynThreadPoolTMqttServer = class(TSynThreadPool)
  protected
    fServer: TMqttServer;
    fBigBodySize: integer;
    fMaxBodyThreadCount: integer;
    {$ifndef USE_WINIOCP}
    function QueueLength: integer; override;
    {$endif USE_WINIOCP}
    procedure Task(aCaller: TSynThread; aContext: Pointer); override;
    procedure TaskAbort(aContext: Pointer); override;
  public
    constructor Create(Server: TMqttServer;NumberOfThreads: integer = 32); reintroduce;
    property BigBodySize: integer read fBigBodySize write fBigBodySize;
    property MaxBodyThreadCount: integer read fMaxBodyThreadCount write fMaxBodyThreadCount;
  end;

  TMqttServer = class(TMqttServerGeneric)
  protected
    fProcessCS: TRTLCriticalSection;
    fHeaderRetrieveAbortDelay: integer;
    fThreadPool: TSynThreadPoolTMqttServer;
    fInternalHttpServerRespList: TSynList;
    fServerConnectionCount: integer;
    fServerConnectionActive: integer;
    fServerKeepAliveTimeOut: cardinal;
    fSockPort: RawUtf8;
    fSock: TCrtSocket;
    fThreadRespClass: TMqttServerRespClass;
    fHttpQueueLength: cardinal;
    fExecuteState: (esNotStarted, esBinding, esRunning, esFinished);
    fStats: array[THttpServerSocketGetRequestResult] of integer;
    fSocketClass: TMqttServerSocketClass;
    fHeadersUnFiltered: boolean;
    fExecuteMessage: string;
    function GetStat(one: THttpServerSocketGetRequestResult): integer;
    function GetHttpQueueLength: cardinal; override;
    procedure SetHttpQueueLength(aValue: cardinal); override;
    procedure InternalHttpServerRespListAdd(resp: TMqttServerResp);
    procedure InternalHttpServerRespListRemove(resp: TMqttServerResp);
    procedure Execute; override;
    procedure OnConnect; virtual;
    procedure OnDisconnect; virtual;
    procedure Process(ClientSock: TMqttServerSocket;ConnectionID:TMqttServerConnectionID; ConnectionThread: TSynThread); virtual;
  public
    constructor Create(const aPort: RawUtf8;
      const OnStart, OnStop: TOnNotifyThread;
      const ProcessName: RawUtf8; ServerThreadPoolCount: integer = 32;
      KeepAliveTimeOut: integer = 30000; aHeadersUnFiltered: boolean = false;
      CreateSuspended: boolean = false); reintroduce; virtual;
    procedure WaitStarted(Seconds: integer = 30); virtual;
    destructor Destroy; override;
    property HeadersUnFiltered: boolean  read fHeadersUnFiltered;
    property Sock: TCrtSocket read fSock;
  published
    /// will contain the current number of connections to the server
    property ServerConnectionActive: integer
      read fServerConnectionActive write fServerConnectionActive;
    /// will contain the total number of connections to the server
    // - it's the global count since the server started
    property ServerConnectionCount: integer
      read fServerConnectionCount write fServerConnectionCount;
    property ServerKeepAliveTimeOut: cardinal  read fServerKeepAliveTimeOut write fServerKeepAliveTimeOut;
    property SockPort: RawUtf8  read fSockPort;
    property ThreadPool: TSynThreadPoolTMqttServer  read fThreadPool;
    property HeaderRetrieveAbortDelay: integer read fHeaderRetrieveAbortDelay write fHeaderRetrieveAbortDelay;
    property StatHeaderErrors: integer  index grError read GetStat;
    /// how many invalid HTTP headers raised an exception
    property StatHeaderException: integer index grException read GetStat;
    /// how many HTTP requests pushed more than MaximumAllowedContentLength bytes
    property StatOversizedPayloads: integer index grOversizedPayload read GetStat;
    /// how many HTTP requests were rejected by the OnBeforeBody event handler
    property StatRejected: integer index grRejected read GetStat;
    /// how many HTTP requests were rejected after HeaderRetrieveAbortDelay timeout
    property StatHeaderTimeout: integer index grTimeout read GetStat;
    /// how many HTTP headers have been processed
    property StatHeaderProcessed: integer index grHeaderReceived read GetStat;
    /// how many HTTP bodies have been processed
    property StatBodyProcessed: integer  index grBodyReceived read GetStat;
    /// how many HTTP connections were passed to an asynchronous handler
    // - e.g. for background WebSockets processing after proper upgrade
    property StatOwnedConnections: integer  index grOwned read GetStat;
  end;  

implementation

{ TMqttServerRequest }

constructor TMqttServerRequest.Create(aServer: TMqttServerGeneric;
  aConnectionID: TMqttServerConnectionID; aConnectionThread: TSynThread);
begin
  inherited Create;
  fServer := aServer;
  fConnectionID := aConnectionID;
  fConnectionThread := aConnectionThread;
end;

var
  GlobalRequestID: integer;

procedure TMqttServerRequest.Prepare(const aURL, aMethod,
  aInHeaders: RawUtf8; const aInContent: RawByteString;
  const aInContentType, aRemoteIP: RawUtf8; aUseSSL: boolean);
var
  id: PInteger;
begin
  if fServer = nil then
    id := @GlobalRequestID
  else
    id := @fServer.fCurrentRequestID;
//  fRequestID := InterLockedIncrement(id^);
//  if fRequestID = maxInt - 2048 then // ensure no overflow (31-bit range)
//    id^ := 0;
  fUseSSL := aUseSSL;
  fRemoteIP := aRemoteIP;
end;

{ TMqttGeneric }

constructor TMqttGeneric.Create(CreateSuspended: boolean; const OnStart,
  OnStop: TOnNotifyThread; const ProcessName: RawUtf8);
begin
  fProcessName := ProcessName;
  fOnHttpThreadStart := OnStart;
  SetOnTerminate(OnStop);
  inherited Create(CreateSuspended);
end;

procedure TMqttGeneric.NotifyThreadStart(Sender: TSynThread);
begin
  if Sender = nil then
    raise EMqttServer.CreateUtf8('%.NotifyThreadStart(nil)', [self]);
  if Assigned(fOnHttpThreadStart) and
     not Assigned(Sender.StartNotified) then
  begin
    fOnHttpThreadStart(Sender);
    Sender.StartNotified := self;
  end;
end;

procedure TMqttGeneric.SetOnTerminate(const Event: TOnNotifyThread);
begin
  fOnThreadTerminate := Event;
end;

{ TMqttServerGeneric }

function TMqttServerGeneric.Callback(Ctxt: TMqttServerRequest;
  aNonBlocking: boolean): cardinal;
begin
  raise EMqttServer.CreateUtf8('%.Callback is not implemented: try to use ' +
    'another communication protocol, e.g. WebSockets', [self]);
end;

constructor TMqttServerGeneric.Create(CreateSuspended: boolean;
  const OnStart, OnStop: TOnNotifyThread; const ProcessName: RawUtf8);
begin
  inherited Create(CreateSuspended, OnStart, OnStop, ProcessName);
end;

function TMqttServerGeneric.NextConnectionID: integer;
begin
  result := InterlockedIncrement(fCurrentConnectionID);
  if result = maxInt - 2048 then // paranoid 31-bit counter reset to ensure >0
    fCurrentConnectionID := 0;
end;

procedure TMqttServerGeneric.SetMaximumAllowedContentLength(
  aMax: cardinal);
begin
  fMaximumAllowedContentLength := aMax;
end;

procedure TMqttServerGeneric.Shutdown;
begin
  if self <> nil then
    fShutdownInProgress := true;
end;

{ TMqttServerSocket }

constructor TMqttServerSocket.Create(aServer: TMqttServer);
begin
  inherited Create(5000);
  if aServer <> nil then // nil e.g. from TRtspOverHttpServer
  begin
    fServer := aServer;
    fSocketLayer := aServer.Sock.SocketLayer;
  end;
end;

function TMqttServerSocket.GetRequest(withBody: boolean;
  headerMaxTix: Int64): THttpServerSocketGetRequestResult;
var
  P: PUtf8Char;
  status: cardinal;
  ContentLength,pending: integer;
  reason, allheaders: RawUtf8;
  noheaderfilter: boolean;

  Command,fURL,Content: RawUtf8;
begin
  result := grError;
  try
    // use SockIn with 1KB buffer if not already initialized: 2x faster
    CreateSockIn;
    // abort now with no exception if socket is obviously broken
    if fServer <> nil then
    begin
      pending := SockInPending(100, {alsosocket=}true);
      if (pending < 0) or
         (fServer = nil) or
         fServer.Terminated then
        exit;
      noheaderfilter := fServer.HeadersUnFiltered;
    end
    else
      noheaderfilter := false;
    // 1st line is command: 'GET /path HTTP/1.1' e.g.
    SockRecvLn(Command);
    P := pointer(Command);
    if P = nil then
      exit; // broken
    GetNextItem(P, ' ', fContent); // 'GET'
    GetNextItem(P, ' ', fURL);    // '/path'
    fKeepAliveClient := ((fServer = nil) or
                         (fServer.ServerKeepAliveTimeOut > 0)) and
                        IdemPChar(P, 'HTTP/1.1');
    Content := '';
    fKeepAliveClient := true;
    if (headerMaxTix > 0) and
       (GetTickCount64 > headerMaxTix) then
    begin
      result := grTimeout;
      exit; // allow 10 sec for header -> DOS/TCPSYN Flood
    end;
    if fServer <> nil then
    begin
      if (length(fContent) > 0) and
         (fServer.MaximumAllowedContentLength > 0) and
         (cardinal(ContentLength) > fServer.MaximumAllowedContentLength) then
      begin
        SockSend('HTTP/1.0 413 Payload Too Large'#13#10#13#10'Rejected');
        SockSendFlush('');
        result := grOversizedPayload;
        exit;
      end;
    end;
    {if withBody and not (hfConnectionUpgrade in HeaderFlags) then
    begin
      if IdemPCharArray(pointer(fMethod), ['HEAD', 'OPTIONS']) < 0 then
        GetBody;
      result := grBodyReceived;
    end
    else   }
      result := grHeaderReceived;
  except
    on E: Exception do
      result := grException;
  end;
end;

{ TMqttServerResp }

constructor TMqttServerResp.Create(aSock: TNetSocket; const aSin: TNetAddr;
  aServer: TMqttServer);
var
  c: TMqttServerSocketClass;
begin
  fClientSock := aSock;
  fClientSin := aSin;
  if aServer = nil then
    c := TMqttServerSocket
  else
    c := aServer.fSocketClass;
  Create(c.Create(aServer), aServer); // on Linux, Execute raises during Create
end;

constructor TMqttServerResp.Create(aServerSock: TMqttServerSocket;
  aServer: TMqttServer);
begin
  fServer := aServer;
  fServerSock := aServerSock;
  fOnThreadTerminate := fServer.fOnThreadTerminate;
  fServer.InternalHttpServerRespListAdd(self);
  fConnectionID := aServerSock.RemoteConnectionID;
  if fConnectionID = 0 then
    fConnectionID := fServer.NextConnectionID; // fallback to 31-bit sequence
  FreeOnTerminate := true;
  inherited Create(false);
end;

procedure TMqttServerResp.Execute;
  procedure HandleRequestsProcess;
  var
    keepaliveendtix, beforetix, headertix, tix: Int64;
    pending: TCrtSocketPending;
    res: THttpServerSocketGetRequestResult;
  begin
    {$ifdef SYNCRTDEBUGLOW}
    try
    {$endif SYNCRTDEBUGLOW}
    try
      repeat
        beforetix := mormot.core.os.GetTickCount64;
        keepaliveendtix := beforetix + fServer.ServerKeepAliveTimeOut;
        repeat
          // within this loop, break=wait for next command, exit=quit
          if (fServer = nil) or
             fServer.Terminated or
             (fServerSock = nil) then
            // server is down -> close connection
            exit;
          pending := fServerSock.SockReceivePending(50); // 50 ms timeout
          if (fServer = nil) or
             fServer.Terminated then
            // server is down -> disconnect the client
            exit;
          {$ifdef SYNCRTDEBUGLOW}
          TSynLog.Add.Log(sllCustom2, 'HandleRequestsProcess: sock=% pending=%',
            [fServerSock.fSock, _CSP[pending]], self);
          {$endif SYNCRTDEBUGLOW}
          case pending of
            cspSocketError:
              exit; // socket error -> disconnect the client
            cspNoData:
              begin
                tix := mormot.core.os.GetTickCount64;
                if tix >= keepaliveendtix then
                  exit; // reached keep alive time out -> close connection
                if tix - beforetix < 40 then
                begin
                  {$ifdef SYNCRTDEBUGLOW}
                  // getsockopt(fServerSock.fSock,SOL_SOCKET,SO_ERROR,@error,errorlen) returns 0 :(
                  TSynLog.Add.Log(sllCustom2,
                    'HandleRequestsProcess: sock=% LOWDELAY=%',
                    [fServerSock.fSock, tix - beforetix], self);
                  {$endif SYNCRTDEBUGLOW}
                  SleepHiRes(1); // seen only on Windows in practice
                  if (fServer = nil) or
                     fServer.Terminated then
                    // server is down -> disconnect the client
                    exit;
                end;
                beforetix := tix;
              end;
            cspDataAvailable:
              begin
                // get request and headers
                headertix := fServer.HeaderRetrieveAbortDelay;
                if headertix > 0 then
                  inc(headertix, beforetix);
                res := fServerSock.GetRequest({withbody=}true, headertix);
                if (fServer = nil) or
                   fServer.Terminated then
                  // server is down -> disconnect the client
                  exit;
                LockedInc32(@fServer.fStats[res]);
                case res of
                  grBodyReceived, grHeaderReceived:
                    begin
                      if res = grBodyReceived then
                        LockedInc32(@fServer.fStats[grHeaderReceived]);
                      // calc answer and send response
                      fServer.Process(fServerSock, ConnectionID, self);
                      // keep connection only if necessary
                      if fServerSock.KeepAliveClient then
                        break
                      else
                        exit;
                    end;
                  grOwned:
                    begin
                      fServerSock := nil; // will be freed by new owner
                      exit;
                    end;
                else
                  // fServerSock connection was down or headers are not correct
                  exit;
                end;
              end;
          end;
        until false;
      until false;
    except
      on E: Exception do
        ; // any exception will silently disconnect the client
    end;
    {$ifdef SYNCRTDEBUGLOW}
    finally
      TSynLog.Add.Log(sllCustom2, 'HandleRequestsProcess: close sock=%',
        [fServerSock.fSock], self);
    end;
    {$endif SYNCRTDEBUGLOW}
  end;

var
  netsock: TNetSocket;
begin
  fServer.NotifyThreadStart(self);
  try
    try
      if fClientSock.Socket <> 0 then
      begin
        // direct call from incoming socket
        netsock := fClientSock;
        fClientSock := nil; // fServerSock owns fClientSock
        fServerSock.AcceptRequest(netsock, @fClientSin);
        if fServer <> nil then
          HandleRequestsProcess;
      end else
      begin
        // call from TSynThreadPoolTHttpServer -> handle first request
//        if not fServerSock.fBodyRetrieved and (IdemPCharArray(pointer(fServerSock.fMethod), ['HEAD', 'OPTIONS']) < 0) then
//          fServerSock.GetBody;
        fServer.Process(fServerSock, ConnectionID, self);
        if (fServer <> nil) and
           fServerSock.KeepAliveClient then
          HandleRequestsProcess; // process further kept alive requests
     end;
    finally
      try
        if fServer <> nil then
        try
          fServer.OnDisconnect;
        finally
          fServer.InternalHttpServerRespListRemove(self);
          fServer := nil;
        end;
      finally
        FreeAndNil(fServerSock);
        // if Destroy happens before fServerSock.GetRequest() in Execute below
        fClientSock.ShutdownAndClose({rdwr=}false);
      end;
    end;
  except
    on Exception do
      ; // just ignore unexpected exceptions here, especially during clean-up
  end;
end;


{ TSynThreadPoolTMqttServer }

constructor TSynThreadPoolTMqttServer.Create(Server: TMqttServer;
  NumberOfThreads: integer);
begin
  fServer := Server;
  fOnThreadTerminate := fServer.fOnThreadTerminate;
  fBigBodySize := THREADPOOL_BIGBODYSIZE;
  fMaxBodyThreadCount := THREADPOOL_MAXWORKTHREADS;
  inherited Create(NumberOfThreads {$ifndef USE_WINIOCP}, {queuepending=}true{$endif});
end;

{$ifndef USE_WINIOCP}
function TSynThreadPoolTMqttServer.QueueLength: integer;
begin
  if fServer = nil then
    result := 10000
  else
    result := fServer.fHttpQueueLength;
end;
{$endif}

procedure TSynThreadPoolTMqttServer.Task(aCaller: TSynThread;
  aContext: Pointer);
var
  srvsock: TMqttServerSocket;
  headertix: Int64;
  res: THttpServerSocketGetRequestResult;
begin
  srvsock := aContext;
  try
    if fServer.Terminated then
      exit;
    // get Header of incoming request in the thread pool
    headertix := fServer.HeaderRetrieveAbortDelay;
    if headertix > 0 then
      headertix := headertix + GetTickCount64;
    res := srvsock.GetRequest({withbody=}false, headertix);
    if (fServer = nil) or
       fServer.Terminated then
      exit;
    // properly get the incoming body and process the request
    LockedInc32(@fServer.fStats[res]);
    case res of
      grHeaderReceived:
        begin
          // connection and header seem valid -> process request further
          if (fServer.ServerKeepAliveTimeOut > 0) and
             (fServer.fInternalHttpServerRespList.Count < fMaxBodyThreadCount) and
             (srvsock.KeepAliveClient or
              (srvsock.ContentLength > fBigBodySize)) then
          begin
            // HTTP/1.1 Keep Alive (including WebSockets) or posted data > 16 MB
            // -> process in dedicated background thread
            fServer.fThreadRespClass.Create(srvsock, fServer);
            srvsock := nil; // THttpServerResp will own and free srvsock
          end
          else
          begin
            // no Keep Alive = multi-connection -> process in the Thread Pool
            {if not (hfConnectionUpgrade in srvsock.HeaderFlags) and
               (IdemPCharArray(pointer(srvsock.fMethod), ['HEAD', 'OPTIONS']) < 0) then
            begin
              srvsock.GetBody; // we need to get it now
              LockedInc32(@fServer.fStats[grBodyReceived]);
            end;    }
            // multi-connection -> process now
            fServer.Process(srvsock, srvsock.RemoteConnectionID, aCaller);
            fServer.OnDisconnect;
            // no Shutdown here: will be done client-side
          end;
        end;
      grOwned:
        // e.g. for asynchrounous WebSockets
        srvsock := nil; // to ignore FreeAndNil(srvsock) below
    end; // errors will close the connection
  finally
    srvsock.Free;
  end;
end;

procedure TSynThreadPoolTMqttServer.TaskAbort(aContext: Pointer);
begin
  TMqttServerSocket(aContext).Free;
end;

{ TMqttServer }

constructor TMqttServer.Create(const aPort: RawUtf8; const OnStart,
  OnStop: TOnNotifyThread; const ProcessName: RawUtf8;
  ServerThreadPoolCount, KeepAliveTimeOut: integer; aHeadersUnFiltered,
  CreateSuspended: boolean);
begin
  fSockPort := aPort;
  fInternalHttpServerRespList := TSynList.Create;
  InitializeCriticalSection(fProcessCS);
  fServerKeepAliveTimeOut := KeepAliveTimeOut; // 30 seconds by default
  if fThreadPool <> nil then
    fThreadPool.ContentionAbortDelay := 5000; // 5 seconds default
  // event handlers set before inherited Create to be visible in childs
  fOnHttpThreadStart := OnStart;
  SetOnTerminate(OnStop);
  if fThreadRespClass = nil then
    fThreadRespClass := TMqttServerResp;
  if fSocketClass = nil then
    fSocketClass := TMqttServerSocket;
  if ServerThreadPoolCount > 0 then
  begin
    fThreadPool := TSynThreadPoolTMqttServer.Create(self, ServerThreadPoolCount);
    fHttpQueueLength := 1000;
  end;
  fHeadersUnFiltered := aHeadersUnFiltered;
  inherited Create(CreateSuspended, OnStart, OnStop, ProcessName);
end;

destructor TMqttServer.Destroy;
var
  endtix: Int64;
  i: PtrInt;
  resp: TMqttServerResp;
  callback: TNetSocket;
begin
  Terminate; // set Terminated := true for THttpServerResp.Execute
  if fThreadPool <> nil then
    fThreadPool.fTerminated := true; // notify background process
  if (fExecuteState = esRunning) and
     (Sock <> nil) then
  begin
    Sock.Close; // shutdown the socket to unlock Accept() in Execute
    if NewSocket('127.0.0.1', Sock.Port, nlTCP, false, 1, 1, 1, 0, callback) = nrOK then
      callback.ShutdownAndClose({rdwr=}false);
  end;
  endtix := mormot.core.os.GetTickCount64 + 20000;
  EnterCriticalSection(fProcessCS);
  try
    if fInternalHttpServerRespList <> nil then
    begin
      for i := 0 to fInternalHttpServerRespList.Count - 1 do
      begin
        resp := fInternalHttpServerRespList.List[i];
        resp.Terminate;
        resp.fServerSock.Sock.ShutdownAndClose({rdwr=}true);
      end;
      repeat // wait for all THttpServerResp.Execute to be finished
        if (fInternalHttpServerRespList.Count = 0) and
           (fExecuteState <> esRunning) then
          break;
        LeaveCriticalSection(fProcessCS);
        SleepHiRes(100);
        EnterCriticalSection(fProcessCS);
      until mormot.core.os.GetTickCount64 > endtix;
      FreeAndNil(fInternalHttpServerRespList);
    end;
  finally
    LeaveCriticalSection(fProcessCS);
    FreeAndNil(fThreadPool); // release all associated threads and I/O completion
    FreeAndNil(fSock);
    inherited Destroy;       // direct Thread abort, no wait till ended
    DeleteCriticalSection(fProcessCS);
  end;
end;

procedure TMqttServer.Execute;
var
  cltsock: TNetSocket;
  cltaddr: TNetAddr;
  cltservsock: TMqttServerSocket;
  res: TNetResult;
  {$ifdef MONOTHREAD}
  endtix: Int64;
  {$endif MONOTHREAD}
begin
  // THttpServerGeneric thread preparation: launch any OnHttpThreadStart event
  fExecuteState := esBinding;
  NotifyThreadStart(self);
  // main server process loop
  try
    fSock := TCrtSocket.Bind(fSockPort); // BIND + LISTEN
    {$ifdef OSLINUX}
    // in case we started by systemd, listening socket is created by another process
    // and do not interrupt while process got a signal. So we need to set a timeout to
    // unblock accept() periodically and check we need terminations
    if fSockPort = '' then // external socket
      fSock.ReceiveTimeout := 1000; // unblock accept every second
    {$endif OSLINUX}
    fExecuteState := esRunning;
    if not fSock.SockIsDefined then // paranoid (Bind would have raise an exception)
      raise EMqttServer.CreateUtf8('%.Execute: %.Bind failed', [self, fSock]);
    while not Terminated do
    begin
      res := Sock.Sock.Accept(cltsock, cltaddr);
      if not (res in [nrOK, nrRetry]) then
        if Terminated then
          break
        else
        begin
          SleepHiRes(1); // failure (too many clients?) -> wait and retry
          continue;
        end;
      if Terminated or
         (Sock = nil) then
      begin
        cltsock.ShutdownAndClose({rdwr=}true);
        break; // don't accept input if server is down
      end;
      OnConnect;
      {$ifdef MONOTHREAD}
      cltservsock := fSocketClass.Create(self);
      try
        cltservsock.InitRequest(cltsock);
        endtix := fHeaderRetrieveAbortDelay;
        if endtix > 0 then
          inc(endtix, mormot.core.os.GetTickCount64);
        if cltservsock.GetRequest({withbody=}true, endtix)
            in [grBodyReceived, grHeaderReceived] then
          Process(cltservsock, 0, self);
        OnDisconnect;
        DirectShutdown(cltsock);
      finally
        cltservsock.Free;
      end;
      {$else}
      if Assigned(fThreadPool) then
      begin
        // use thread pool to process the request header, and probably its body
        cltservsock := fSocketClass.Create(self);
        cltservsock.AcceptRequest(cltsock, @cltaddr);
        if not fThreadPool.Push(pointer(PtrUInt(cltservsock)),
            {waitoncontention=}true) then
        begin
          // returned false if there is no idle thread in the pool, and queue is full
          cltservsock.Free; // will call DirectShutdown(cltsock)
        end;
      end
      else
        // default implementation creates one thread for each incoming socket
        fThreadRespClass.Create(cltsock, cltaddr, self);
      {$endif MONOTHREAD}
    end;
  except
    on E: Exception do
      // any exception would break and release the thread
      fExecuteMessage := E.ClassName + ' [' + E.Message + ']';
  end;
  EnterCriticalSection(fProcessCS);
  fExecuteState := esFinished;
  LeaveCriticalSection(fProcessCS);
end;

function TMqttServer.GetHttpQueueLength: cardinal;
begin
  result := fHttpQueueLength;;
end;

function TMqttServer.GetStat(
  one: THttpServerSocketGetRequestResult): integer;
begin
  result := fStats[one];
end;

procedure TMqttServer.InternalHttpServerRespListAdd(resp: TMqttServerResp);
begin
  if (self = nil) or
     (fInternalHttpServerRespList = nil) or
     (resp = nil) then
    exit;
  EnterCriticalSection(fProcessCS);
  try
    fInternalHttpServerRespList.Add(resp);
  finally
    LeaveCriticalSection(fProcessCS);
  end;
end;

procedure TMqttServer.InternalHttpServerRespListRemove(
  resp: TMqttServerResp);
var
  i: integer;
begin
  if (self = nil) or
     (fInternalHttpServerRespList = nil) then
    exit;
  EnterCriticalSection(fProcessCS);
  try
    i := fInternalHttpServerRespList.IndexOf(resp);
    if i >= 0 then
      fInternalHttpServerRespList.Delete(i);
  finally
    LeaveCriticalSection(fProcessCS);
  end;
end;

procedure TMqttServer.OnConnect;
begin
  LockedInc32(@fServerConnectionCount);
  LockedInc32(@fServerConnectionActive);
end;

procedure TMqttServer.OnDisconnect;
begin
  LockedDec32(@fServerConnectionActive);
end;

procedure TMqttServer.Process(ClientSock: TMqttServerSocket;
  ConnectionID: TMqttServerConnectionID; ConnectionThread: TSynThread);
var
  ctxt: TMqttServerRequest;
  respsent: boolean;
  cod, aftercode: cardinal;
  reason: RawUtf8;
  errmsg: string;

  function SendFileAsResponse: boolean;
  var
    fn: TFileName;
  begin
    result := true;
    ctxt.fOutContent := 'abc';
  end;

  function SendResponse: boolean;
  var
    P, PEnd: PUtf8Char;
    len: PtrInt;
  begin
    result := not Terminated; // true=success
    if not result then
      exit;
    {$ifdef SYNCRTDEBUGLOW}
    TSynLog.Add.Log(sllCustom2, 'SendResponse respsent=% cod=%', [respsent,
      cod], self);
    {$endif SYNCRTDEBUGLOW}
    respsent := true;
    StatusCodeToReason(cod, reason);
    if errmsg <> '' then
    begin
      ctxt.fOutContent := FormatUtf8('<body style="font-family:verdana">'#10 +
        '<h1>% Server Error %</h1><hr><p>HTTP % %<p>%<p><small>',
        [self, cod, cod, reason, HtmlEscapeString(errmsg)]);
    end;
    // 1. send HTTP status command
    if ClientSock.KeepAliveClient then
      ClientSock.SockSend(['HTTP/1.1 ', cod, ' ', reason]);
    // 2. send headers
    if ClientSock.KeepAliveClient then
    begin
      ClientSock.SockSend('Connection: Keep-Alive'#13#10); // #13#10 -> end headers
    end else
      ClientSock.SockSendCRLF; // headers must end with a void line
    // 3. sent HTTP body content (if any)
    ClientSock.SockSendFlush(ctxt.OutContent); // flush all data to network
  end;

begin
  if (ClientSock = nil) or (ClientSock.fContent = '') then
    // we didn't get the request = socket read error
    exit; // -> send will probably fail -> nothing to send back
  if Terminated then
    exit;
  ctxt := TMqttServerRequest.Create(self, ConnectionID, ConnectionThread);
  try
    respsent := false;
    with ClientSock do
      ctxt.Prepare('', '', 'ip', Content, 'xx',
        '', ClientSock.fTLS);
    try
      //cod := Request(ctxt); // this is the main processing callback
      //aftercode := DoAfterRequest(ctxt);
      {$ifdef SYNCRTDEBUGLOW}
      TSynLog.Add.Log(sllCustom2, 'Request=% DoAfterRequest=%', [cod, aftercode], self);
      {$endif SYNCRTDEBUGLOW}

      {$ifdef SYNCRTDEBUGLOW}
      TSynLog.Add.Log(sllCustom2, 'DoAfterResponse respsent=% errmsg=%',
        [respsent, errmsg], self);
      {$endif SYNCRTDEBUGLOW}
    except
      on E: Exception do
        if not respsent then
        begin
          // notify the exception as server response
          FormatString('%: %', [E, E.Message], errmsg);
          cod := HTTP_SERVERERROR;
          SendResponse;
        end;
    end;
  finally
    // add transfert stats to main socket
    if Sock <> nil then
    begin
      EnterCriticalSection(fProcessCS);
      Sock.BytesIn := Sock.BytesIn + ClientSock.BytesIn;
      Sock.BytesOut := Sock.BytesOut + ClientSock.BytesOut;
      LeaveCriticalSection(fProcessCS);
      ClientSock.fBytesIn := 0;
      ClientSock.fBytesOut := 0;
    end;
    ctxt.Free;
  end;
end;

procedure TMqttServer.SetHttpQueueLength(aValue: cardinal);
begin
  fHttpQueueLength := aValue;
end;

procedure TMqttServer.WaitStarted(Seconds: integer);
var
  tix: Int64;
  ok: boolean;
begin
  tix := mormot.core.os.GetTickCount64 + Seconds * 1000; // never wait forever
  repeat
    EnterCriticalSection(fProcessCS);
    ok := Terminated or
          (fExecuteState in [esRunning, esFinished]);
    LeaveCriticalSection(fProcessCS);
    if ok then
      exit;
    Sleep(1); // warning: waits typically 1-15 ms on Windows
    if mormot.core.os.GetTickCount64 > tix then
      raise EMqttServer.CreateUtf8('%.WaitStarted timeout after % seconds [%]',
        [self, Seconds, fExecuteMessage]);
  until false;
end;

end.

unit MqttServer;

interface

{$I ..\mormot.defines.inc}

uses
  sysutils,
  classes,
  mormot.core.base,
  mormot.core.os,
  mormot.core.rtti,
  mormot.core.data,
  mormot.core.text,
  mormot.core.unicode,
  mormot.core.datetime,
  mormot.core.buffers,
  mormot.core.threads,
  mormot.core.log,
  mormot.net.sock,
  mormot.net.http,
  mormot.net.client,
  mormot.net.server,
  mormot.net.async;

type
  ERtspOverHttp = class(ESynException);

  TPostConnection = class(TAsyncConnection)
  protected
    FLeftBuf:RawUtf8;
    FSubTitle:TRawUtf8DynArray;
    procedure DoParse(temp:RawUtf8);
    function OnRead(
      Sender: TAsyncConnections): TPollAsyncSocketOnRead; override;
    procedure BeforeDestroy(Sender: TAsyncConnections); override;
  end;

  TRtspOverHttpServer = class(TAsyncServer)
  protected
    fRtspServer: RawUtf8;
    function ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean; override;
  public
    constructor Create(const aRtspServer, aHttpPort: RawUtf8;
      aLog: TSynLogClass; const aOnStart, aOnStop: TOnNotifyThread;
      aOptions: TAsyncConnectionsOptions = []); reintroduce;
    destructor Destroy; override;
  end;


implementation

uses MqttUtils;


{ TPostConnection }

function TPostConnection.OnRead(
  Sender: TAsyncConnections): TPollAsyncSocketOnRead;
var
  temp:RawUtf8; 
    function _getframelen:integer;
    var
      bitx,achar,idx:byte;
    begin
      Result := 0;
      bitx := 0;
      idx := 2;
      repeat
        if idx>length(temp) then
        begin
          Result := -3;
          exit;
        end;
        achar := ord(temp[idx]);
        inc(Result,(achar AND $7f) shl bitx);
        if Result>CON_MQTT_MAXBYTE then
        begin
           Result := -1;
           exit;
        end;
        inc(bitx,8);
        if bitx>32 then
        begin
           Result := -2;
           exit;
        end;
      until ( (achar and $80)=0);
    end;
var
  aconn: TAsyncConnection;
  alen,p:integer;
  aframe:RawUtf8;
begin
  result := sorContinue;
  temp := fSlot.readbuf;
  if FLeftBuf <>'' then
    temp := FLeftBuf + temp;
  if Length(temp)<2 then
  begin
    FLeftBuf := temp;
    exit;
  end;
  alen := _getframelen;
  if (alen = -3) or (alen>length(temp)) then
  begin
     FLeftBuf := temp;
     exit;
  end else
  if alen <=0 then exit;

  aframe := copy(temp,1,alen);
  DoParse(aframe);
  aconn := Sender.ConnectionFindLocked(self.Handle);
  if aconn <> nil then
  try
    Sender.Write(aconn, 'ok');
  finally
    Sender.Unlock;
  end else
  begin
    Sender.Log.Add.Log(sllDebug, 'OnRead % ',[Handle], self);
    result := sorClose;
  end;
end;

procedure TPostConnection.BeforeDestroy(Sender: TAsyncConnections);
begin
  inherited BeforeDestroy(Sender);
end;

procedure TPostConnection.DoParse(temp: RawUtf8);
begin
  case TMQTTMessageType(ord(temp[1]) shr 4) of
     mtPINGREQ:
        begin

        end;
     mtSUBSCRIBE:
        begin

        end;
  end;
end;

{ TRtspOverHttpServer }

constructor TRtspOverHttpServer.Create(
  const aRtspServer, aHttpPort: RawUtf8; aLog: TSynLogClass;
  const aOnStart, aOnStop: TOnNotifyThread; aOptions: TAsyncConnectionsOptions);
begin
  fLog := aLog;
  fRtspServer := aRtspServer;
  inherited Create(
    aHttpPort, aOnStart, aOnStop, TPostConnection, 'rtsp/http', aLog, aOptions);
end;

destructor TRtspOverHttpServer.Destroy;
var
  log: ISynLog;
begin
  log := fLog.Enter(self, 'Destroy');
  inherited Destroy;
end;

function TRtspOverHttpServer.ConnectionCreate(aSocket: TNetSocket;
  const aRemoteIp: RawUtf8; out aConnection: TAsyncConnection): boolean;
var
  log: ISynLog;
  sock: TMqttSocket;
  postconn: TPostConnection;
begin
  aConnection := nil;
  result := false;
  log := fLog.Enter('ConnectionCreate(%)', [PtrUInt(aSocket)], self);
  try
    sock := TMqttSocket.Create(5000);
    try
      sock.AcceptRequest(aSocket, nil);
      sock.RemoteIP := aRemoteIp;
      sock.CreateSockIn; // faster header process (released below once not needed)
      if sock.GetLogined then
      begin
        if log <> nil then
          log.Log(sllTrace, 'ConnectionCreate received =%,%',
            [sock.User, sock.Paswd], self);
         sock.Sock := TNetSocket(-1); // disable Close on sock.Free -> handled in pool
      end else
      begin
        if log <> nil then
          log.Log(sllDebug, 'ConnectionCreate: ignored invalid %', [sock], self);
        exit;
      end;
    finally
      sock.Free;
    end;

    postconn := TPostConnection.Create(aRemoteIp);
    postconn.FLeftBuf := '';
    if not inherited ConnectionAdd(aSocket, postconn) then
      raise ERtspOverHttp.CreateUtf8('inherited %.ConnectionAdd(%) failed',
        [self, aSocket]);
    aConnection := postconn;

    result := true;
    if log <> nil then
      log.Log(sllTrace,
        'ConnectionCreate added post=%/% for %',
        [PtrUInt(aSocket), aConnection.Handle], self);
  except
    if log <> nil then
      log.Log(sllDebug, 'ConnectionCreate(%) failed', [PtrUInt(aSocket)], self);
  end;
end;


end.


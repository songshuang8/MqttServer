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

  TMQTTConnection = class(TAsyncConnection)
  protected
    FLeftBuf:RawUtf8;
    FSubTitle:TShortStringDynArray;
    FErrorCount:Integer;
    function DoParse(Sender: TAsyncConnections;mStart:Integer):integer;
    function OnRead(
      Sender: TAsyncConnections): TPollAsyncSocketOnRead; override;
    procedure BeforeDestroy(Sender: TAsyncConnections); override;
    procedure OnLastOperationIdle(Sender: TAsyncConnections);override;

    procedure WriteSelfAck(Sender: TAsyncConnections;p:Pointer;len:integer);
    procedure WriteSelfAck2(Sender: TAsyncConnections;p:RawByteString);
    procedure WritePublic(Sender: TAsyncConnections;t,c:RawByteString);
  public
    constructor Create(const aRemoteIP: RawUtf8); override;
  end;

  TMQTTServer = class(TAsyncServer)
  protected
    fBlackBook:TRawUtf8List;
    function ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean; override;
  public
    constructor Create(const aHttpPort: RawUtf8;
      aLog: TSynLogClass; const aOnStart, aOnStop: TOnNotifyThread;
      aOptions: TAsyncConnectionsOptions = []); reintroduce;
    destructor Destroy; override;
  end;


implementation

uses MqttUtils;


{ TPostConnection }

procedure TMQTTConnection.OnLastOperationIdle(Sender: TAsyncConnections);
begin
  inherited;

end;

function TMQTTConnection.OnRead(
  Sender: TAsyncConnections): TPollAsyncSocketOnRead;
var
  alen,lenwid:integer;
begin
  result := sorContinue;
  if Length(fSlot.readbuf)<2 then
    exit;
  Sender.Log.Add.Log(sllCustom1, 'OnRead %',[BinToHex(fSlot.readbuf)], self);
  alen := mqtt_getframelen(@fSlot.readbuf[2],length(fSlot.readbuf)-1,lenwid);
  if (alen = -3) or (alen>length(fSlot.readbuf)) then
  begin
     exit
  end else
  if alen < 0 then
  begin
   fSlot.readbuf := '';
   inc(FErrorCount);
   exit;
  end;
  if DoParse(Sender,lenwid+2)>=0 then
    FErrorCount := 0
  else
    inc(FErrorCount);
  fSlot.readbuf := '';
end;

procedure TMQTTConnection.WritePublic(Sender: TAsyncConnections; t,c: RawByteString);
var
  i:integer;
  aconn: TMQTTConnection;
  frame:RawByteString;
begin
  frame := #$30 + mqtt_get_lenth(length(t)+length(c)+2) + mqtt_get_strlen(length(t)) + t + c;
  Sender.Lock;
  try
    for i := 0 to Sender.ConnectionCount-1 do
    begin
      aconn := Sender.Connection[i] as TMQTTConnection;
      if mqtt_compareTitle(t,aconn.FSubTitle) then
        Sender.Write(aconn, frame);
    end;
  finally
    Sender.Unlock;
  end;
end;

procedure TMQTTConnection.WriteSelfAck(Sender: TAsyncConnections;p:Pointer; len: integer);
var
  aconn: TAsyncConnection;
begin
  aconn := Sender.ConnectionFindLocked(self.Handle);
  if aconn <> nil then
  try
    Sender.Write(aconn, p,len);
  finally
    Sender.Unlock;
  end else
  begin
    Sender.Log.Add.Log(sllDebug, 'OnRead % ',[Handle], self);
  end;
end;

procedure TMQTTConnection.WriteSelfAck2(Sender: TAsyncConnections; p: RawByteString);
var
  aconn: TAsyncConnection;
begin
  aconn := Sender.ConnectionFindLocked(self.Handle);
  if aconn <> nil then
  try
    Sender.Write(aconn, p);
  finally
    Sender.Unlock;
  end else
  begin
    Sender.Log.Add.Log(sllDebug, 'OnRead % ',[Handle], self);
  end;
end;

procedure TMQTTConnection.BeforeDestroy(Sender: TAsyncConnections);
begin
  inherited BeforeDestroy(Sender);
end;

constructor TMQTTConnection.Create(const aRemoteIP: RawUtf8);
begin
  inherited;
  FErrorCount := 0;
end;

function TMQTTConnection.DoParse(Sender: TAsyncConnections;mStart:Integer):Integer;
var
  identistrid:RawByteString;
  i,ret,tag:integer;
  t:RawByteString;
begin
  Result := 0;
  case TMQTTMessageType(ord(fSlot.readbuf[1]) shr 4) of
     mtPINGREQ:
        begin
          if (length(fSlot.readbuf)<>2)
              or  ((ord(fSlot.readbuf[1]) and $f)<>0)
              or  (ord(fSlot.readbuf[2])<>0) then
          begin
            Result := -1; //error found close it
            exit;
          end;
          WriteSelfAck2(Sender,#$D0#0);    //1010 0000  0
        end;
     mtPUBLISH:
        begin
          tag := ord(fSlot.readbuf[1]) and $f;
          if (tag  and 3)=3 then
          begin
            Result := -1;
            exit;
          end;
          Delete(fSlot.readbuf,1,mStart-1);
          ret := mqtt_readstr(@fSlot.readbuf[1],length(fSlot.readbuf),t);
          if ret=0 then exit;
          Delete(fSlot.readbuf,1,ret);
          if (tag  and 3)>0 then
          begin
            identistrid := fSlot.readbuf[1] + fSlot.readbuf[2];
            Delete(fSlot.readbuf,1,2);
          end;
          if length(fSlot.readbuf)=0 then
            exit;
          WritePublic(Sender,t,fSlot.readbuf);
          if (tag  and 3)>0 then
            WriteSelfAck2(Sender,#$40#02+identistrid);
        end;
     mtSUBSCRIBE:
        begin
          if (ord(fSlot.readbuf[1]) and 3)<>2 then
          begin
            Result := -1; //error found close it
            exit;
          end;
          identistrid := fSlot.readbuf[mStart] + fSlot.readbuf[mStart+1];
          inc(mStart);
          Delete(fSlot.readbuf,1,mStart);
          ret := mqtt_AddTopic(fSlot.readbuf,FSubTitle);
          if ret>0 then
          begin
            t := '';
            for i := 0 to ret-1 do
              t := t + #2;
            WriteSelfAck2(Sender,mqtt_get_subAck(identistrid,t));
            Result := 1;
          end;
        end;
     mtUNSUBSCRIBE:
        begin
          if (ord(fSlot.readbuf[1]) and 3)<>2 then
          begin
            Result := -1; //error found close it
            exit;
          end;
          identistrid := fSlot.readbuf[mStart] + fSlot.readbuf[mStart+1];
          inc(mStart);
          Delete(fSlot.readbuf,1,mStart);
          ret := mqtt_DelTopic(fSlot.readbuf,FSubTitle);
          if ret>0 then
            WriteSelfAck2(Sender,#$b0#02+identistrid);
        end;
  end;
end;

{ TRtspOverHttpServer }

constructor TMQTTServer.Create(
  const aHttpPort: RawUtf8; aLog: TSynLogClass;
  const aOnStart, aOnStop: TOnNotifyThread; aOptions: TAsyncConnectionsOptions);
begin
  fLog := aLog;
  fBlackBook := TRawUtf8List.Create([fObjectsOwned, fCaseSensitive]);
  inherited Create(
    aHttpPort, aOnStart, aOnStop, TMQTTConnection, 'rtsp/http', aLog, aOptions);
end;

destructor TMQTTServer.Destroy;
var
  log: ISynLog;
begin
  log := fLog.Enter(self, 'Destroy');
  inherited Destroy;
end;

function TMQTTServer.ConnectionCreate(aSocket: TNetSocket;
  const aRemoteIp: RawUtf8; out aConnection: TAsyncConnection): boolean;
var
  log: ISynLog;
  sock: TMqttSocket;
  postconn: TMQTTConnection;
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

    postconn := TMQTTConnection.Create(aRemoteIp);
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


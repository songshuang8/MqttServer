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
    FSubTitle:TRawUtf8DynArray;
    function DoParse(Sender: TAsyncConnections;mStart:Integer):integer;
    function OnRead(
      Sender: TAsyncConnections): TPollAsyncSocketOnRead; override;
    procedure BeforeDestroy(Sender: TAsyncConnections); override;

    procedure WriteSelfAck(Sender: TAsyncConnections;p:Pointer;len:integer);
    procedure WriteSelfAck2(Sender: TAsyncConnections;p:RawByteString);
    procedure WritePublic(Sender: TAsyncConnections;t,body:RawByteString);
  end;

  TMQTTServer = class(TAsyncServer)
  protected
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

function TMQTTConnection.OnRead(
  Sender: TAsyncConnections): TPollAsyncSocketOnRead;
var
  alen,lenwid:integer;
    procedure _getframelen;
    var
      bitx,achar:byte;
    begin
      alen := 0;
      bitx := 0;
      lenwid := 0;
      repeat
        if (lenwid+2)>length(fSlot.readbuf) then
        begin
          alen := -3;
          exit;
        end;
        achar := ord(fSlot.readbuf[lenwid+2]);
        inc(alen,(achar AND $7f) shl bitx);
        if alen>CON_MQTT_MAXBYTE then
        begin
           alen := -1;
           exit;
        end;
        inc(bitx,7);
        if bitx>21 then
        begin
           alen := -2;
           exit;
        end;
        inc(lenwid);
      until ( (achar and $80)=0);
    end;
var
  aframe:RawUtf8;
begin
  result := sorContinue;
  if Length(fSlot.readbuf)<2 then
    exit;

  _getframelen;
  if (alen = -3) or (alen>length(fSlot.readbuf)) then
     exit
  else  if alen < 0 then
  begin
   fSlot.readbuf := '';
   exit;
  end;
  DoParse(Sender,lenwid+2);
  fSlot.readbuf := '';
end;

procedure TMQTTConnection.WritePublic(Sender: TAsyncConnections; t,body: RawByteString);
var
  i:integer;
  aconn: TMQTTConnection;
begin
  Sender.Lock;
  try
    for i := 0 to Sender.ConnectionCount-1 do
    begin
      aconn := Sender.Connection[i] as TMQTTConnection;
      Sender.Write(aconn, body);
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

function TMQTTConnection.DoParse(Sender: TAsyncConnections;mStart:Integer):Integer;
var
  identifierid:word;
  identistrid:RawByteString;
  i,ret,tag:integer;
  t:RawUtf8;
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
          ret := mqtt_readstr(@fSlot.readbuf[mStart],length(fSlot.readbuf)-mStart+1,t);
          if ret=0 then exit;
          i := mStart + ret;
          if (tag  and 3)>0 then
          begin
            identistrid := fSlot.readbuf[i] + fSlot.readbuf[i+1];
            Delete(fSlot.readbuf,i,2);
          end;
          if length(fSlot.readbuf)-i+1=0 then
            exit;
          fSlot.readbuf[1] := AnsiChar($30);
          WritePublic(Sender,t,fSlot.readbuf);
          if (tag  and 3)>0 then
          begin
            WriteSelfAck2(Sender,#$40#02+identistrid);
          end;
        end;
     mtSUBSCRIBE:
        begin
          if (ord(fSlot.readbuf[1]) and 3)<>2 then
          begin
            Result := -1; //error found close it
            exit;
          end;
          identifierid := ord(fSlot.readbuf[mStart]) shl 8 + ord(fSlot.readbuf[mStart+1]);
          inc(mStart);
          Delete(fSlot.readbuf,1,mStart);
          ret := mqtt_readTopic(fSlot.readbuf,FSubTitle);
          if ret>0 then
          begin
            t := '';
            for i := 0 to ret-1 do
              t := t + #2;
            WriteSelfAck2(Sender,mqtt_get_subAck(identifierid,t));
            Result := 1;
          end;
        end;
  end;
end;

{ TRtspOverHttpServer }

constructor TMQTTServer.Create(
  const aHttpPort: RawUtf8; aLog: TSynLogClass;
  const aOnStart, aOnStop: TOnNotifyThread; aOptions: TAsyncConnectionsOptions);
begin
  fLog := aLog;
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


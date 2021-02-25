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
  mormot.net.async_rw;

type
  ERtspOverHttp = class(ESynException);

  TMQTTConnection = class(TAsyncConnection)
  private
    FWillTopic,FWillMessage:RawByteString;
    FSubTitle:TShortStringDynArray;  
  protected
    function DoParsePacket(Sender: TAsyncConnections;MsgType:Byte;Payload:RawByteString):integer;

    function OnRead(Sender: TAsyncConnections): TPollAsyncSocketOnRead; override;
    procedure BeforeDestroy(Sender: TAsyncConnections); override;

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
    procedure OnClientClose(connection: TObject); override;
  public
    constructor Create(const aHttpPort: RawUtf8;
      aLog: TSynLogClass; const aOnStart, aOnStop: TOnNotifyThread;
      aOptions: TAsyncConnectionsOptions = []); reintroduce;
    destructor Destroy; override;
  end;


implementation

uses MqttUtils;


{ TPostConnection }

function TMQTTConnection.DoParsePacket(Sender: TAsyncConnections;MsgType:Byte;Payload:RawByteString):Integer;
var
  identistrid:RawByteString;
  i,ret:integer;
  t:RawByteString;
begin
  Result := 0;
  case TMQTTMessageType(MsgType shr 4) of
     mtPINGREQ:
        begin
          if ((MsgType and $f)<>0)
             or (Payload<>'') then
          begin
            Result := -1; //error found close it
            exit;
          end;
          WriteSelfAck2(Sender,#$D0#0);    //1010 0000  0
        end;
     mtPUBLISH:
        begin
          if ((MsgType  and 3)=3) or (length(Payload)<4) then
          begin
            Result := -1;
            exit;
          end;
          ret := mqtt_readstr(@Payload[1],length(Payload),t);
          if ret=0 then
          begin
            Result := -2;
            exit;
          end;
          Delete(Payload,1,ret);
          if (MsgType  and 3)>0 then
          begin
            if length(Payload)<3 then
            begin
              Result := -3;
              exit;
            end;
            identistrid := Payload[1] + Payload[2];
            Delete(Payload,1,2);
          end;
          if length(Payload)=0 then
          begin
            Result := -4;
            exit;
          end;
          WritePublic(Sender,t,Payload);
          if (MsgType  and 3)>0 then
            WriteSelfAck2(Sender,#$40#02+identistrid);
        end;
     mtSUBSCRIBE:
        begin
          if ((MsgType and 3)<>2)
            or (length(Payload)<5) then
          begin
            Result := -1; //error found close it
            exit;
          end;
          identistrid := Payload[1] + Payload[2];
          Delete(Payload,1,2);
          ret := mqtt_AddTopic(Payload,FSubTitle);
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
          if ((MsgType and 3)<>2) or (length(Payload)<5) then
          begin
            Result := -1; //error found close it
            exit;
          end;
          identistrid := Payload[1] + Payload[2];
          Delete(Payload,1,2);
          ret := mqtt_DelTopic(Payload,FSubTitle);
          if ret>0 then
            WriteSelfAck2(Sender,#$b0#02+identistrid);
        end;
  end;
end;

function TMQTTConnection.OnRead(
  Sender: TAsyncConnections): TPollAsyncSocketOnRead;
var
  alen,lenwid:integer;
  Payload:RawByteString;
  MsgType:Byte;
begin
  result := sorContinue;
  if length(fSlot.readbuf)='' then exit;
  Sender.Log.Add.Log(sllCustom1, 'OnRead %',[BinToHex(fSlot.readbuf)], self);
  if
  while length(fSlot.readbuf)>=2 do
  begin
    alen := mqtt_getframelen(@fSlot.readbuf[2],length(fSlot.readbuf)-1,lenwid);
    if (alen = -3) or ((alen+lenwid+1)>length(fSlot.readbuf)) then  //part data, wait for recv
    begin
       exit;
    end else
    if alen < 0 then
    begin
       fSlot.readbuf := '';
       result := sorClose;     //error
       exit;
    end;
    MsgType := ord(fSlot.readbuf[1]);
    if alen>0 then
      Payload := Copy(fSlot.readbuf,lenwid+2,alen)
    else
      Payload := '';
    Delete(fSlot.readbuf,1,alen + lenwid + 1);
    if DoParsePacket(Sender,MsgType,Payload)<0 then
    begin
       fSlot.readbuf := '';
       result := sorClose;     //error
       exit;
    end;
  end;
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
      //if aconn=self then Continue;
      if mqtt_compareTitle(t,aconn.FSubTitle) then
        Sender.Write(aconn, frame);
    end;
  finally
    Sender.Unlock;
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
  if FWillTopic<>'' then
    WritePublic(Sender,FWillTopic,FWillMessage);
end;

constructor TMQTTConnection.Create(const aRemoteIP: RawUtf8);
begin
  inherited;
end;

{ TRtspOverHttpServer }

constructor TMQTTServer.Create(
  const aHttpPort: RawUtf8; aLog: TSynLogClass;
  const aOnStart, aOnStop: TOnNotifyThread; aOptions: TAsyncConnectionsOptions);
begin
  fLog := aLog;
  fBlackBook := TRawUtf8List.Create([fObjectsOwned, fCaseSensitive]);
  inherited Create(aHttpPort, aOnStart, aOnStop, TMQTTConnection, 'MQTTSvr', aLog, aOptions,10);
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
  mqttconn: TMQTTConnection;
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
         //
         mqttconn := TMQTTConnection.Create(aRemoteIp);
         mqttconn.LastOperationIdleSeconds := sock.KeepliveTimeOut;
         mqttconn.FWillTopic := sock.WillTopic;
         mqttconn.FWillMessage := sock.WillMessage;
          if not inherited ConnectionAdd(aSocket, mqttconn) then
            raise ERtspOverHttp.CreateUtf8('inherited %.ConnectionAdd(%) failed',
              [self, aSocket]);
          aConnection := mqttconn;

          result := true;
          if log <> nil then
            log.Log(sllTrace,
              'ConnectionCreate added post=%/% for %',
              [PtrUInt(aSocket), aConnection.Handle], self);
            end else
            begin
              if log <> nil then
                log.Log(sllDebug, 'ConnectionCreate: ignored invalid %', [sock], self);
              exit;
            end;
    finally
      sock.Free;
    end;
  except
    if log <> nil then
      log.Log(sllDebug, 'ConnectionCreate(%) failed', [PtrUInt(aSocket)], self);
  end;
end;    

procedure TMQTTServer.OnClientClose(connection: TObject);
begin
  Log.Add.Log(sllCustom1, 'Connection Count:%', [ConnectionCount], self);
end;

end.


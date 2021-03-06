unit MqttService;

interface

{$I mormot.defines.inc}

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
    FClientId:RawByteString;
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
  published
    property ClientId:RawByteString read FClientId;
  end;

  TMQTTServer = class(TAsyncServer)
  protected
    function ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean; override;
    procedure OnClientClose(connection: TObject); override;
    procedure OnClientsLogs(Level: TSynLogInfo; const Fmt: RawUtf8; const Args: array of const; Instance: TObject = nil);
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
  msg:TMQTTMessageType;
  FFlag:byte;
begin
  Result := 0;
  msg := TMQTTMessageType(MsgType shr 4);
  if not Loined then
     if msg<>mtCONNECT then
     begin
       Result := -1;
       exit;
     end;
  case msg of
     mtCONNECT:
        begin
          if Payload[1]<>#0 then
          begin
            Result := -1;
            exit;
          end;
          Delete(Payload,1,1);
          if Payload[1]=MQTT_VERSIONLEN3_CHAR then
          begin
              if length(Payload)<11 then  exit;
              if (Payload[8]<>MQTT_VERSION3_CHAR) then exit;
              if not CompareMemSmall(@Payload[2],PAnsiChar(MQTT_PROTOCOLV3),MQTT_VERSIONLEN3) then exit;
              CanIdleLen := (ord(Payload[10]) shl 8) or ord(Payload[11]);
              FFlag := ord(Payload[9]);
              Delete(Payload,1,11);
          end else
          if (Payload[1]=MQTT_VERSIONLEN311_CHAR) then
          begin
              if length(Payload)<9 then  exit;
              if (Payload[6]<>MQTT_VERSION4_CHAR) then exit;
              if not CompareMemSmall(@Payload[2],PAnsiChar(MQTT_PROTOCOLV311),length(MQTT_PROTOCOLV311)) then exit;
              CanIdleLen := (ord(Payload[8]) shl 8) or ord(Payload[9]) ;
              FFlag := ord(Payload[7]);
              Delete(Payload,1,9);
          end else
          begin
              Result := -1;
              exit;
          end;
          CanIdleLen := fCanIdleLen  + 3;
          ret:=mqtt_readstr(@Payload[1],length(Payload),FClientID);
          if ret = 0 then
          begin
              Result := -1;
              exit;
          end;
          Delete(Payload,1,ret);

          if (FFlag and $4)<>0 then
          begin
            ret:=mqtt_readstr(@Payload[1],length(Payload),FWillTopic);
            if ret = 0 then
            begin
                Result := -1;
                exit;
            end;
            Delete(Payload,1,ret);

            ret:=mqtt_readstr(@Payload[1],length(Payload),FWillMessage);
            if ret = 0 then
            begin
                Result := -1;
                exit;
            end;
            Delete(Payload,1,ret);
          end;

          if (FFlag and $80)<>0 then
          begin
            ret:=mqtt_readstr(@Payload[1],length(Payload),identistrid);
            if ret = 0 then
            begin
                Result := -1;
                exit;
            end;
            Delete(Payload,1,ret);
          end;

          if (FFlag and $40)<>0 then
          begin
            ret:=mqtt_readstr(@Payload[1],length(Payload),identistrid);
            if ret = 0 then
            begin
                Result := -1;
                exit;
            end;
            Delete(Payload,1,ret);
          end;
          WriteSelfAck2(Sender,mqtt_get_conack(0));
          Loined := true;
        end;
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
  Payload,bak:RawByteString;
  MsgType:Byte;
begin
  result := sorContinue;
  if fSlot.readbuf='' then exit;

  bak := fSlot.readbuf;
  while length(fSlot.readbuf)>=2 do
  begin
    alen := mqtt_getframelen(@fSlot.readbuf[2],length(fSlot.readbuf)-1,lenwid);
    if (alen = -3) or ((alen+lenwid+1)>length(fSlot.readbuf)) then  //part data, wait for recv
    begin
       exit;
    end else
    if alen < 0 then
    begin
       Sender.Log.Add.Log(sllCustom1, 'Len is Err ip=%id=%OnRead %',[RemoteIP,FClientId,BinToHex(fSlot.readbuf)], self);
       fSlot.readbuf := '';
       result := sorClose;     //error
       exit;
    end;
    MsgType := ord(fSlot.readbuf[1]);
    if Loined then
    begin
      if alen>2048 then
      begin
         Sender.Log.Add.Log(sllCustom1, 'Len is big ip=%OnRead %',[RemoteIP,BinToHex(fSlot.readbuf)], self);
         fSlot.readbuf := '';
         result := sorClose;     //error
         exit;
      end;
    end else
    begin
      if alen>1024 then
      begin
         Sender.Log.Add.Log(sllCustom1, 'Len is big ip=%OnRead %',[RemoteIP,BinToHex(fSlot.readbuf)], self);
         fSlot.readbuf := '';
         result := sorClose;     //error
         exit;
      end;
    end;
    if alen>0 then
      Payload := Copy(fSlot.readbuf,lenwid+2,alen)
    else
      Payload := '';
    Delete(fSlot.readbuf,1,alen + lenwid + 1);
    if DoParsePacket(Sender,MsgType,Payload)<0 then
    begin
       Sender.Log.Add.Log(sllCustom1, 'Jiexi is Err ip=%id=%OnRead %',[RemoteIP,FClientId,BinToHex(bak)], self);
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
//  if FWillTopic<>'' then
//    WritePublic(Sender,FWillTopic,FWillMessage);
end;

constructor TMQTTConnection.Create(const aRemoteIP: RawUtf8);
begin
  inherited;
  FClientId:= '';
end;

{ TRtspOverHttpServer }

constructor TMQTTServer.Create(
  const aHttpPort: RawUtf8; aLog: TSynLogClass;
  const aOnStart, aOnStop: TOnNotifyThread; aOptions: TAsyncConnectionsOptions);
begin
  fLog := aLog;
  inherited Create(aHttpPort, aOnStart, aOnStop, TMQTTConnection, 'MQTTSvr', aLog, aOptions,22);
  ServerSocket.OnLog := OnClientsLogs;
end;

destructor TMQTTServer.Destroy;
var
  log: ISynLog;
begin
  log := fLog.Enter(self, 'Destroy');
  inherited Destroy;
end;

function TMQTTServer.ConnectionCreate(aSocket: TNetSocket; const aRemoteIp: RawUtf8;
      out aConnection: TAsyncConnection): boolean;
begin
  aConnection := TMQTTConnection.Create(aRemoteIp);
  aConnection.CanIdleLen := 3;      // connected in 3 sec,
  if not inherited ConnectionAdd(aSocket, aConnection) then
  begin
     aConnection.Free;
     aConnection := nil;
     Log.Add.Log(sllCustom1, 'inherited %.ConnectionAdd(%) failed', [ConnectionCount], self);
     Result := false;
     exit;
  end;
  Log.Add.Log(sllCustom1, 'Open Connection Count:%', [ConnectionCount], self);
  result := true;
end;    

procedure TMQTTServer.OnClientClose(connection: TObject);
begin
  Log.Add.Log(sllCustom1, 'Close Connection Count:%', [ConnectionCount], self);
end;


procedure TMQTTServer.OnClientsLogs(Level: TSynLogInfo; const Fmt: RawUtf8;
  const Args: array of const; Instance: TObject);
begin
  Log.Add.Log(Level, Fmt, Args, Instance);
end;

end.


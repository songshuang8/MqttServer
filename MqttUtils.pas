unit MqttUtils;

interface

uses
  sysutils,
  classes,
  mormot.core.base,
  mormot.core.os,
  mormot.core.rtti,
  mormot.core.data,
  mormot.core.text,
  mormot.core.unicode,
  mormot.core.log,
  mormot.net.sock;

type
  TMQTTMessageType =
  (
    mtBROKERCONNECT,  //  0	Broker request to connect to Broker
    mtCONNECT,        //	1	Client request to connect to Broker
    mtCONNACK,        //	2	Connect Acknowledgment
    mtPUBLISH,        //	3	Publish message
    mtPUBACK,         //	4	Publish Acknowledgment
    mtPUBREC,         //	5	Publish Received (assured delivery part 1)
    mtPUBREL,         //	6	Publish Release (assured delivery part 2)
    mtPUBCOMP,        //	7	Publish Complete (assured delivery part 3)
    mtSUBSCRIBE,      //	8	Client Subscribe request
    mtSUBACK,         //	9	Subscribe Acknowledgment
    mtUNSUBSCRIBE,    // 10	Client Unsubscribe request
    mtUNSUBACK,       // 11	Unsubscribe Acknowledgment
    mtPINGREQ,        // 12	PING Request
    mtPINGRESP,       // 13	PING Response
    mtDISCONNECT,     // 14	Client is Disconnecting
    mtReserved15      // 15
  );

const
  CON_MQTT_MAXBYTE = 2048;
  CON_MQTT_MAXSUB = 5;
  MQTT_PROTOCOLV3   = 'MQIsdp';
  MQTT_VERSION3    = 3;
  MQTT_VERSIONLEN3    = 6;
  MQTT_PROTOCOLV311   = 'MQTT';
  MQTT_VERSION4    = 4;
  MQTT_VERSIONLEN311    = 4;
  MsgNames : array [TMQTTMessageType] of string =
  (
    'BROKERCONNECT',	//  0	Broker request to connect to Broker
    'CONNECT',        //	1	Client request to connect to Broker
    'CONNACK',        //	2	Connect Acknowledgment
    'PUBLISH',        //	3	Publish message
    'PUBACK',         //	4	Publish Acknowledgment
    'PUBREC',         //	5	Publish Received (assured delivery part 1)
    'PUBREL',         //	6	Publish Release (assured delivery part 2)
    'PUBCOMP',        //	7	Publish Complete (assured delivery part 3)
    'SUBSCRIBE',      //	8	Client Subscribe request
    'SUBACK',         //	9	Subscribe Acknowledgment
    'UNSUBSCRIBE',    // 10	Client Unsubscribe request
    'UNSUBACK',       // 11	Unsubscribe Acknowledgment
    'PINGREQ',        // 12	PING Request
    'PINGRESP',       // 13	PING Response
    'DISCONNECT',     // 14	Client is Disconnecting
    'Reserved15'      // 15
  );

  qtAT_MOST_ONCE=0;   //  0 At most once Fire and Forget        <=1
  qtAT_LEAST_ONCE=1;  //  1 At least once Acknowledged delivery >=1
  qtAT_EXACTLY_ONCE=2;   //  2 Exactly once Assured delivery       =1
  qtAT_Reserved3=3;	      //  3	Reserved
  QOSNames : array [0..3] of string =
  (
    'AT_MOST_ONCE',   //  0 At most once Fire and Forget        <=1
    'AT_LEAST_ONCE',  //  1 At least once Acknowledged delivery >=1
    'EXACTLY_ONCE',   //  2 Exactly once Assured delivery       =1
    'RESERVED'	      //  3	Reserved
  );

type
  TMqttSocket = class(TCrtSocket)
  protected
    FUser,FPaswd,FClientID,FWillTopic,FWillMessage:RawUtf8;
    FVer,FFlag:byte;
    FKeeplive:word;
  public
    function GetLogined:boolean;
    constructor Create(aTimeOut: PtrInt = 10000); reintroduce;

    property User:RawUtf8 read FUser;
    property Paswd:RawUtf8 read FPaswd;
    property ClientID:RawUtf8 read FClientID;
    property WillTopic:RawUtf8 read FWillTopic;
    property WillMessage:RawUtf8 read FWillMessage;
  end;

function mqtt_readstr(p:Pointer;leftlen:integer;out sstr:RawUtf8):integer;
function mqtt_readTopic(const buf:RawByteString;out topics:TRawUtf8DynArray):integer;

function mqtt_get_subAck(identifierid:word;returnCode:RawUtf8):RawUtf8;

implementation

function mqtt_swaplen(alen:word):word;
begin
  Result := ((alen and $ff) shl 8) + (alen shr 8) and $ff;
end;

function mqtt_gethdr(MsgType: TMQTTMessageType; Dup: Boolean;Qos: Integer; Retain: Boolean):AnsiChar;
begin
  { Fixed Header Spec:
    bit	   |7 6	5	4	    | |3	     | |2	1	     |  |  0   |
    byte 1 |Message Type| |DUP flag| |QoS level|	|RETAIN| }
  Result := AnsiChar((Ord(MsgType) shl 4) + (ord(Dup) shl 3) + (ord (Qos) shl 1) + ord (Retain));
end;

function mqtt_add_lenth(p:Pointer;alen:integer):integer;
begin
  Result := 0;
  repeat
    PAnsiChar(p)^ := AnsiChar(alen and $ff);
    alen := alen shr 8;
    inc(PtrUInt(p),1);
    inc(Result,1);
  until (alen = 0);
end;

function mqtt_get_conAck(returCode:Byte):RawUtf8;
begin
  SetLength(Result,4);
  Result[1] := mqtt_gethdr(mtCONNACK, false, qtAT_MOST_ONCE, false);
  mqtt_add_lenth(@Result[2],2);
  Result[3] := #0;
  Result[4] := AnsiChar(returCode);
end;

function mqtt_get_subAck(identifierid:word;returnCode:RawUtf8):RawUtf8;
begin
  SetLength(Result,4);
  Result[1] := mqtt_gethdr(mtSUBACK, false, qtAT_MOST_ONCE, false);
  mqtt_add_lenth(@Result[2],length(returnCode)+2);
  Result[3] := AnsiChar(identifierid shr 8);
  Result[4] := AnsiChar(identifierid and $ff);
  Result := Result + returnCode;
end;

function mqtt_readstr(p:Pointer;leftlen:integer;out sstr:RawUtf8):integer;
var
  alen:word;
begin
  Result := 0;
  Move(p^,alen,2);
  alen := mqtt_swaplen(alen);
  if (alen=0) or ((alen+2)>leftlen) then exit;
  inc(PtrUInt(p),2);
  SetLength(sstr,alen);
  MoveFast(p^,sstr[1],alen);
  Result := alen + 2;
end;

function mqtt_readTopic(const buf:RawByteString;out topics:TRawUtf8DynArray):Integer;
var
  temp:RawUtf8;
  t,n:integer;
  mQos:byte;
begin
  Result := 0;
  t := 1;
  repeat
    n := mqtt_readstr(@buf[t],length(buf)-t + 1,temp);
    if n=0 then break;
    SetLength(topics,Result+1);
    topics[Result] := temp;
    inc(Result);
    if Result >= CON_MQTT_MAXSUB then
    begin
      Result := -2;
      exit;
    end;
    inc(t,n);
     // qos byte
    mQos := ord(buf[t]);
    inc(t);
  until (t>length(buf));
end;

{ TMqttSocket }

constructor TMqttSocket.Create(aTimeOut: PtrInt);
begin
  inherited Create(aTimeOut);
  FUser := '';
  FClientID := '';
  FPaswd := '';
  FWillTopic := '';
end;

function TMqttSocket.GetLogined: boolean;
var
  achar:byte;
    function _getframelen:integer;
    var
      bitx:byte;
    begin
      Result := 0;
      bitx := 0;
      repeat
        SockRecv(@achar,1);
        inc(Result,(achar AND $7f) shl bitx);
        if Result>CON_MQTT_MAXBYTE then
        begin
           Result := -1;
           exit;
        end;
        inc(bitx,7);
        if bitx>21 then
        begin
           Result := -2;
           exit;
        end;
      until ( (achar and $80)=0);
    end;
var
  alen,p:integer;
  buf:array of byte;
begin
  result := false;
  try
    CreateSockIn;
    //
    SockRecv(@achar,1);
    if TMQTTMessageType((achar and $f0) shr 4) <> mtCONNECT then
      exit;
    alen := _getframelen;
    if alen<10 then  exit;
    //
{|Protocol Name
|byte 1     |Length MSB (0)         |0 |0 |0 |0 |0 |0 |0 |0
|byte 2     |Length LSB (4)         |0 |0 |0 |0 |0 |1 |0 |0
|byte 3     |¡®M¡¯                    |0 |1 |0 |0 |1 |1 |0 |1
|byte 4     |¡®Q¡¯                    |0 |1 |0 |1 |0 |0 |0 |1
|byte 5     |¡®T¡¯                    |0 |1 |0 |1 |0 |1 |0 |0
|byte 6     |¡®T¡¯                    |0 |1 |0 |1 |0 |1 |0 |0
|Protocol Level
|byte 7     |Level (4)              |0 |0 |0 |0 |0 |1 |0 |0
|Connect Flags
|byte 8     |User Name Flag (1)     |1 |1 |0 |0 |1 |1 |1 |0
|           |Password Flag (1)
|           |Will Retain (0)
|           |Will QoS (01)
|           |Will Flag (1)
|           |Clean Session (1)
|           |Reserved (0)
|Keep Alive
|byte 9     |Keep Alive MSB (0)     |0 |0 |0 |0 |0 |0 |0 |0
|byte 10    |Keep Alive LSB (10)    |0 |0 |0 |0 |1 |0 |1 |0}
    ///
    SetLength(buf,alen);
    SockRecv(@buf[0],alen);
    if buf[0] <> 0 then exit;
    if (buf[1]=MQTT_VERSIONLEN3) then
    begin
      if alen<12 then  exit;
      if (buf[8]<>MQTT_VERSION3) then exit;
      if not CompareMemSmall(@buf[2],PAnsiChar(MQTT_PROTOCOLV3),MQTT_VERSIONLEN3) then exit;
      FKeeplive := (buf[10] shl 8) or buf[11];
      p := 9;
    end else
    if (buf[1]=MQTT_VERSIONLEN311) then
    begin
      if (buf[6]<>MQTT_VERSION4) then exit;
      if not CompareMemSmall(@buf[2],PAnsiChar(MQTT_PROTOCOLV311),length(MQTT_PROTOCOLV311)) then exit;
      FKeeplive := (buf[8] shl 8) or buf[9];
      p := 7;
    end else
      exit;
    FFlag := buf[p];
    inc(p,3);
    p := p + mqtt_readstr(@buf[p],alen-p,FClientID);
    if (FFlag and $4)<>0 then
    begin
      p := p + mqtt_readstr(PAnsiChar(buf[p]),alen-p,FWillTopic);
      p := p + mqtt_readstr(PAnsiChar(buf[p]),alen-p,FWillMessage);
    end;
    if (FFlag and $80)<>0 then
      p := p + mqtt_readstr(@buf[p],alen-p,FUser);
    if (FFlag and $40)<>0 then
      mqtt_readstr(@buf[p],alen-p,FPaswd);
    Write(mqtt_get_conack(0));
    result := true;
  except
  end;
end;

end.

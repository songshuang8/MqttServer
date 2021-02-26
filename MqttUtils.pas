unit MqttUtils;

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
//  MQTT_VERSION3    = 3;
  MQTT_VERSION3    = #3;
  MQTT_VERSIONLEN3    = 6;
  MQTT_VERSIONLEN3_CHAR    = #6;
  MQTT_PROTOCOLV311   = 'MQTT';
//  MQTT_VERSION4    = 4;
  MQTT_VERSIONLEN311_CHAR    = #4;
  MQTT_VERSION4    = #4;
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
    fLog: TSynLogClass;
    FUser,FPaswd,FClientID,FWillTopic,FWillMessage:RawByteString;
    FVer,FFlag:byte;
    FKeepliveTimeOut:word;
  public
    function GetLogined:boolean;
    constructor Create(aTimeOut: PtrInt;alog:TSynLogClass); reintroduce;

    property User:RawByteString read FUser;
    property Paswd:RawByteString read FPaswd;
    property ClientID:RawByteString read FClientID;
    property WillTopic:RawByteString read FWillTopic;
    property WillMessage:RawByteString read FWillMessage;
    property KeepliveTimeOut:word read FKeepliveTimeOut;
  end;

function mqtt_getframelen(p:Pointer;len:integer;out lenbyte:integer):integer;
function mqtt_readstr(p:Pointer;leftlen:integer;out sstr:RawByteString):integer;

function mqtt_AddTopic(const buf:RawByteString;var topics:TShortStringDynArray):integer;
function mqtt_DelTopic(const buf:RawByteString;var topics:TShortStringDynArray):integer;
function mqtt_compareTitle(const at:RawByteString;topics:TShortStringDynArray):boolean;

function mqtt_get_lenth(alen:integer):RawByteString;
function mqtt_get_strlen(strlen:integer):RawByteString;

function mqtt_get_subAck(identifierid:RawByteString;returnCode:RawByteString):RawByteString;
function mqtt_get_unsubAck(identifierid:RawByteString;returnCode:RawByteString):RawByteString;
function mqtt_get_conAck(returCode:Byte):RawByteString;

implementation

function mqtt_getframelen(p:Pointer;len:integer;out lenbyte:integer):integer;
var
  bitx,achar:byte;
begin
  Result := 0;
  bitx := 0;
  lenbyte := 0;
  while True do
  BEGIN
    inc(Result,(pbyte(p)^ AND $7f) shl bitx);
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
    inc(lenbyte);
    if lenbyte>len then
    begin
      Result := -3;
      exit;
    end;
    if (pbyte(p)^ AND $80) = 0 then
      break;
    inc(PtrUInt(p));
  end;
end;

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
    PAnsiChar(p)^ := AnsiChar(alen and $7f);
    alen := alen shr 7;
    inc(PtrUInt(p),1);
    inc(Result,1);
  until (alen = 0);
end;

function mqtt_get_lenth(alen:integer):RawByteString;
var
  n:byte;
begin
  Result := '';
  while True do
  begin
    n := alen and $7f;
    alen := alen shr 7;
    if(alen<>0) then
      inc(n,$80);
    Result := Result + AnsiChar(n);
    if alen=0 then
      break;
  end;
end;

function mqtt_get_strlen(strlen:integer):RawByteString;
begin
  Result := AnsiChar((strlen shr 8) and $ff) + AnsiChar(strlen and $ff);
end;

function mqtt_get_conAck(returCode:Byte):RawByteString;
begin
  SetLength(Result,4);
  Result[1] := mqtt_gethdr(mtCONNACK, false, qtAT_MOST_ONCE, false);
  mqtt_add_lenth(@Result[2],2);
  Result[3] := #0;
  Result[4] := AnsiChar(returCode);
end;

function mqtt_get_subAck(identifierid:RawByteString;returnCode:RawByteString):RawByteString;
begin
  SetLength(Result,2);
  Result[1] := mqtt_gethdr(mtSUBACK, false, qtAT_MOST_ONCE, false);
  mqtt_add_lenth(@Result[2],length(returnCode)+2);
  Result := Result + identifierid + returnCode;
end;

function mqtt_get_unsubAck(identifierid:RawByteString;returnCode:RawByteString):RawByteString;
begin
  SetLength(Result,2);
  Result[1] := mqtt_gethdr(mtUNSUBACK, false, qtAT_MOST_ONCE, false);
  mqtt_add_lenth(@Result[2],length(returnCode)+2);
  Result := Result + identifierid + returnCode;
end;

function mqtt_readstr(p:Pointer;leftlen:integer;out sstr:RawByteString):integer;
var
  alen:word;
begin
  Result := 0;
  MoveFast(PAnsiChar(p)^,alen,2);
  alen := mqtt_swaplen(alen);
  if (alen=0) or ((alen+2)>leftlen) then exit;
  inc(PtrUInt(p),2);
  SetLength(sstr,alen);
  MoveFast(PAnsiChar(p)^,sstr[1],alen);
  Result := alen + 2;
end;

function mqtt_AddTopic(const buf:RawByteString;var topics:TShortStringDynArray):Integer;
    procedure _append(const atopic:RawByteString);
    var
      i:integer;
    begin
      for i:=0 to high(topics) do
      if topics[i]=atopic then
        exit;
      i := length(topics);
      SetLength(topics,i+1);
      topics[i] := atopic;
    end;
var
  temp:RawByteString;
  t,n:integer;
  mQos:byte;
begin
  Result := 0;
  t := 1;
  repeat
    n := mqtt_readstr(@buf[t],length(buf)-t + 1,temp);
    if n<2 then break;
    _append(temp);
    if length(topics) >= CON_MQTT_MAXSUB then
    begin
      Result := -2;
      exit;
    end;
    inc(t,n);
     // qos byte
    mQos := ord(buf[t]);
    inc(t);
    inc(Result);
  until (t>length(buf));
end;

function mqtt_DelTopic(const buf:RawByteString;var topics:TShortStringDynArray):integer;
    procedure _del(const atopic:RawByteString);
    var
      i,j,n:integer;
    begin
      for i:=0 to high(topics) do
      if topics[i]=atopic then
      begin
        n := length(topics) - 1;
        for j:=i+1 to n do
          topics[j-1] := topics[j];
        SetLength(topics,n);
        exit;
      end;
    end;
var
  temp:RawByteString;
  t,n:integer;
  mQos:byte;
begin
  Result := 0;
  t := 1;
  repeat
    n := mqtt_readstr(@buf[t],length(buf)-t + 1,temp);
    if n<2 then break;
    _del(temp);
    inc(t,n);
    inc(Result);
  until (t>length(buf));
end;

function mqtt_compareTitle(const at:RawByteString;topics:TShortStringDynArray):boolean;
  function _compareTitle(const src,dst:RawByteString):boolean;      // is not standard
  var
    temp,temp2:RawByteString;
  begin
    Result := true;
    if src = dst then exit;
    if (dst[length(dst)] = '#') then
    begin
      temp := Copy(dst,1,length(dst)-1);
      if src = temp then exit;
      if length(temp)=0 then exit;  //  all
      if temp[length(temp)] = '/' then
      begin
        temp := Copy(dst,1,length(dst)-1);
        if pos(temp,src) = 1 then exit;
      end;
    end else
    if (dst[length(dst)] = '+') then
    begin
      temp := Copy(dst,1,length(dst)-1);  //  /+
      if length(src) > length(temp) then
      begin
        temp2 := src;
        Delete(temp2,1,length(temp));
        if Pos('/',temp2)<1 then
          exit;
      end;
    end;
    Result := false;
  end;
var
  i:integer;
begin
  Result := true;
  for i := 0 to High(topics) do
  if _compareTitle(at,topics[i]) then
    exit;
  Result := false;
end;

{ TMqttSocket }

constructor TMqttSocket.Create(aTimeOut: PtrInt;alog:TSynLogClass);
begin
  inherited Create(aTimeOut);
  fLog := alog;
  FUser := '';
  FClientID := '';
  FPaswd := '';
  FWillTopic := '';
end;

//const
//  teststr = '00064D514973647003C60009000C64633466323235383063666600197075622F686561746374726C2F646334663232353830636666000A7B22636D64223A38387D000474657374000474657374';

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
  alen,p,n:integer;
  buf:array of byte;
//  temp:RawUtf8;
begin
  result := false;
  try
    CreateSockIn;
    //
    SockRecv(@achar,1);
    if TMQTTMessageType((achar and $f0) shr 4) <> mtCONNECT then
    begin
      fLog.Add.Log(sllCustom1, 'Login title=%',[inttostr(achar)], self);
      exit;
    end;
    alen := _getframelen;
    if alen<10 then
    begin
      fLog.Add.Log(sllCustom1, 'Login len=%',[alen], self);
      exit;
    end;
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
    if buf[0] <> 0 then
    begin
      fLog.Add.Log(sllCustom1, 'Login %',[BinToHex(@buf[0],alen)], self);
      exit;
    end;
//    temp := teststr;
//    alen := length(temp) div 2;
//    SetLength(buf,alen);
//    for p := 1 to High(buf) do
//    begin
//      buf[p-1] := StrToInt('$'+temp[1]+ temp[2]);
//      delete(temp,1,2);
//    end;

//    if (buf[1]=MQTT_VERSIONLEN3) then
//    begin
//      if alen<12 then  exit;
//      if (buf[8]<>MQTT_VERSION3) then exit;
//      if not CompareMemSmall(@buf[2],PAnsiChar(MQTT_PROTOCOLV3),MQTT_VERSIONLEN3) then exit;
//      FKeepliveTimeOut := (buf[10] shl 8) or buf[11];
//      p := 9;
//    end else
//    if (buf[1]=MQTT_VERSIONLEN311) then
//    begin
//      if (buf[6]<>MQTT_VERSION4) then exit;
//      if not CompareMemSmall(@buf[2],PAnsiChar(MQTT_PROTOCOLV311),length(MQTT_PROTOCOLV311)) then exit;
//      FKeepliveTimeOut := (buf[8] shl 8) or buf[9];
//      p := 7;
//    end else
//    begin
//      fLog.Add.Log(sllCustom1, 'Login other ver%',[BinToHex(@buf[0],alen)], self);
//      exit;
//    end;
//    FFlag := buf[p];
//    inc(p,3);
//    n:=mqtt_readstr(@buf[p],alen-p,FClientID);
//    if n = 0 then
//    begin
//      fLog.Add.Log(sllCustom1, 'Login read err 1 %',[BinToHex(@buf[0],alen)], self);
//      exit;
//    end;
//    p := p + n;
//
//    if (FFlag and $4)<>0 then
//    begin
//      n := mqtt_readstr(@(buf[p]),alen-p,FWillTopic);
//      if n = 0 then
//      begin
//        fLog.Add.Log(sllCustom1, 'Login read err 2 %',[BinToHex(@buf[0],alen)], self);
//        exit;
//      end;
//      p := p + n;
//
//      n := mqtt_readstr(@(buf[p]),alen-p,FWillMessage);
//      if n = 0 then
//      begin
//        fLog.Add.Log(sllCustom1, 'Login read err 3 %',[BinToHex(@buf[0],alen)], self);
//        exit;
//      end;
//      p := p + n;
//    end;
//    if (FFlag and $80)<>0 then
//    begin
//      n := mqtt_readstr(@(buf[p]),alen-p,FUser);
//      if n = 0 then
//      begin
//        fLog.Add.Log(sllCustom1, 'Login read err 4 %',[BinToHex(@buf[0],alen)], self);
//        exit;
//      end;
//      p := p + n;
//    end;
//
//    if (FFlag and $40)<>0 then
//    begin
//      n := mqtt_readstr(@(buf[p]),alen-p,FPaswd);
//      if n = 0 then
//      begin
//        fLog.Add.Log(sllCustom1, 'Login read err 5 %',[BinToHex(@buf[0],alen)], self);
//        exit;
//      end;
//      p := p + n;
//    end;
//    Write(mqtt_get_conack(0));
//    result := true;
  except
  end;
end;

end.

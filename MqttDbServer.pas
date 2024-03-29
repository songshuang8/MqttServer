unit MqttDbServer;

interface

{$I mormot.defines.inc}

uses
  sysutils,
  classes,
  variants,
  contnrs,
  mormot.core.base,
  mormot.core.os,
  mormot.core.data,
  mormot.core.text,
  mormot.core.log,
  mormot.core.json,
  mormot.rest.sqlite3,
  mormot.db.raw.sqlite3,
  mormot.rest.server,
  mormot.rest.core,
  mormot.orm.core,
  MqttService;

type
  TSQLRecordTimeStamped = class(TOrm)
  private
    fCreatedAt: TCreateTime;
    fModifiedAt: TModTime;
  published
    property CreatedAt: TCreateTime read fCreatedAt write fCreatedAt;
    property ModifiedAt: TModTime read fModifiedAt write fModifiedAt;
  end;

  TSQLMessage = class(TSQLRecordTimeStamped)
  private
    fTitle,fBody:RawByteString;
  public
  published
    property Title: RawByteString read fTitle write fTitle;
    property Body: RawByteString read fBody write fBody;
  end;

  TMQTTHttpServer = class(TSQLRestServerDB)
  protected
    fmqttserver:TMQTTServer;
    function OnBeforeIrData(Ctxt: TRestServerUriContext): boolean;
    procedure OnAfterExecProc(Ctxt: TRestServerUriContext);
  public
    constructor MyCreate(aModel:TOrmModel;mqttserver:TMQTTServer;const aDBFileName: TFileName);
  published
    procedure getclients(Ctxt: TRestServerUriContext);
    procedure getclientcount(Ctxt: TRestServerUriContext);

    procedure getblackip(Ctxt: TRestServerUriContext);
  end;

function CreateMyModel: TOrmModel;

implementation

function CreateMyModel: TOrmModel;
begin
  result := TOrmModel.Create([TSQLMessage],'root');
  Result.SetCustomCollationForAll(oftUtf8Text,'NOCASE');
end;

{ TMQTTHttpServer }

procedure TMQTTHttpServer.getblackip(Ctxt: TRestServerUriContext);
var
  i:integer;
  //sstr:ShortString;
begin
  fmqttserver.BlackBook.Safe.Lock;
  try
    //sstr := PPShortString(PPAnsiChar(fmqttserver.BlackBook)^ + vmtClassName)^^;
    Ctxt.Returns(fmqttserver.BlackBook);
  finally
    fmqttserver.BlackBook.Safe.Unlock;
  end;
end;

procedure TMQTTHttpServer.getclientcount(Ctxt: TRestServerUriContext);
begin
  fmqttserver.Lock;
  try
    Ctxt.Returns(Int32ToUtf8(fmqttserver.ConnectionCount));
  finally
    fmqttserver.Unlock;
  end;
end;

procedure TMQTTHttpServer.getclients(Ctxt: TRestServerUriContext);
begin
  fmqttserver.Lock;
  try
    Ctxt.Returns(ObjArrayToJson(fmqttserver.Connection,[woDontStoreDefault,woEnumSetsAsText]));
  finally
    fmqttserver.Unlock;
  end;
end;

constructor TMQTTHttpServer.MyCreate(aModel:TOrmModel;mqttserver:TMQTTServer;const aDBFileName: TFileName);
begin
  inherited Create(aModel,aDBFileName,false);
  fmqttserver := mqttserver;
 // OnBeforeURI := {$ifdef FPC}@{$endif}OnBeforeIrData;
//  OnAfterURI := {$ifdef FPC}@{$endif}OnAfterExecProc;
//  ServiceDefine(TServiceCalculator,[ICalculator],sicShared);
end;

procedure TMQTTHttpServer.OnAfterExecProc(Ctxt: TRestServerUriContext);
begin
  //
end;

function TMQTTHttpServer.OnBeforeIrData(Ctxt: TRestServerUriContext): boolean;
begin
  Result := (fmqttserver<>nil);
end;

end.

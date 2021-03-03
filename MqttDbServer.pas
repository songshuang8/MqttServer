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
  mormot.core.log,
  mormot.rest.sqlite3,
  mormot.db.raw.sqlite3,
  mormot.rest.server,
  mormot.rest.core,
  mormot.orm.core;

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
    FBackSuc,FBackBusing:Boolean;
    FBackTotal,FBackFin:Integer;
    function OnBeforeIrData(Ctxt: TSQLRestServerURIContext): boolean;
    procedure OnAfterExecProc(Ctxt: TSQLRestServerURIContext);
  public
    constructor MyCreate(aModel:TOrmModel; const aDBFileName: TFileName);
  published
  end;

function CreateMyModel: TOrmModel;

implementation

function CreateMyModel: TOrmModel;
begin
  result := TOrmModel.Create([TSQLMessage],'root');
  Result.SetCustomCollationForAll(oftUtf8Text,'NOCASE');
end;

{ TMQTTHttpServer }

constructor TMQTTHttpServer.MyCreate(aModel: TOrmModel;
  const aDBFileName: TFileName);
begin
  inherited Create(aModel,aDBFileName,false);
//  OnBeforeURI := {$ifdef FPC}@{$endif}OnBeforeIrData;
//  OnAfterURI := {$ifdef FPC}@{$endif}OnAfterExecProc;
//  ServiceDefine(TServiceCalculator,[ICalculator],sicShared);
end;

procedure TMQTTHttpServer.OnAfterExecProc(Ctxt: TSQLRestServerURIContext);
begin
  //
end;

function TMQTTHttpServer.OnBeforeIrData(
  Ctxt: TSQLRestServerURIContext): boolean;
begin
  //
end;

end.

unit MqttApp;

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
  mormot.net.async_rw,
  MqttServer;

type  
  TMQTTApp = class
  protected
    mqttserver: TMQTTServer;
  public
    constructor Create;
    destructor Destroy; override;

    function init:Boolean;
  end;

implementation

{ TMQTTApp }

constructor TMQTTApp.Create;
begin
  with TSynLog.Family do
  begin
    HighResolutionTimeStamp := true;
    PerThreadLog := ptIdentifiedInOnFile;
    Level := [sllTrace];
//    Level := LOG_STACKTRACE + [sllCustom1];
    EchoToConsole := LOG_STACKTRACE + [sllCustom1];
    DestinationPath := 'log'+PathDelim;
    if not FileExists(DestinationPath) then  CreateDir(DestinationPath);
    LocalTimestamp:=true;
    HighResolutionTimestamp := false;
    //NoFile := true;
    //EchoCustom := OnLogEvent;
    EchoToConsole := LOG_VERBOSE; // log all events to the console
  end;
  mqttserver := TMQTTServer.Create('4010', TSynLog, nil, nil);
  mqttserver.Clients.Options := [paoWritePollOnly];
  //server.Options := [acoVerboseLog];
  writeln(mqttserver.ClassName, ' running');
  writeln('  performing tests with ', ' concurrent streams using ',
      mqttserver.Clients.PollRead.PollClass.ClassName, #10);
end;

destructor TMQTTApp.Destroy;
begin
  mqttserver.Free;
  inherited;
end;

function TMQTTApp.init: Boolean;
begin
  Result := true;
end;

end.

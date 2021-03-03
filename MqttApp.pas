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
  mormot.db.raw.sqlite3.static,
  mormot.db.raw.sqlite3,
  mormot.orm.core,
  mormot.net.async_rw,
  mormot.rest.http.server,
  MqttDbServer,
  MqttService ;

type
  TMQTTApp = class
  protected
    Fmqttserver: TMQTTServer;
    Fdberver:TMQTTHttpServer;
    fServer: TSQLHttpServer;
    fmodle:TSQLModel;
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
//    Level := [sllTrace];
    Level := LOG_STACKTRACE + [sllCustom1];
    EchoToConsole := LOG_STACKTRACE + [sllCustom1];
    DestinationPath := 'log'+PathDelim;
    if not FileExists(DestinationPath) then  CreateDir(DestinationPath);
    LocalTimestamp:=true;
    HighResolutionTimestamp := false;
    //NoFile := true;
    //EchoCustom := OnLogEvent;
    EchoToConsole := LOG_VERBOSE; // log all events to the console
  end;

  Fmqttserver := TMQTTServer.Create('4009', TSynLog, nil, nil);
  Fmqttserver.WaitStarted;
  Fmqttserver.Clients.Options := [paoWritePollOnly];
  //server.Options := [acoVerboseLog];
  writeln(Fmqttserver.ClassName, ' running');
  writeln('  performing tests with ', ' concurrent streams using ',
      Fmqttserver.Clients.PollRead.PollClass.ClassName, #10);
  //
  fmodle := CreateMyModel;
  Fdberver := TMQTTHttpServer.MyCreate(fmodle,ChangeFileExt(ExeVersion.ProgramFilePath,'data/remotedb.db'));
  Fdberver.DB.Synchronous := smNormal;
  Fdberver.DB.LockingMode := lmExclusive;   //lmNormal;//     lmExclusive
  Fdberver.DB.UseCache := true;
  //FAdminServer.DB.Execute('VACUUM');

  Fdberver.CreateMissingTables;

  fServer := TSQLHttpServer.Create('65501',[Fdberver],'+',useHttpSocket,1);//,32,secSSL);65500
  fServer.AccessControlAllowOrigin := '*';
  fServer.AccessControlAllowCredential := true;

  fServer.RootRedirectToURI('root/Default'); // redirect / to blog/default
end;

destructor TMQTTApp.Destroy;
begin
  fServer.Shutdown;
  FreeAndNil(fServer);

  Fmqttserver.Free;

  Fdberver.Free;
  fmodle.Free;
  inherited;
end;

function TMQTTApp.init: Boolean;
begin
  Result := true;
end;

end.

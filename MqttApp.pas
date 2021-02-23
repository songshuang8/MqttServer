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
  MqttServer;

type  
  TRestMqttServer = class(TSynPersistentLock)
  protected
    fShutdownInProgress: boolean;
    fHttpServer: TMqttServer;
    fPort, fDomainName: RawUtf8;
    fPublicPort: RawUtf8;
    /// internal servers to compute responses (protected by inherited fSafe)
    fLog: TSynLogClass;
    procedure HttpThreadStart(Sender: TThread); virtual;
    procedure HttpThreadTerminate(Sender: TThread); virtual;
  public
    constructor Create(const aPort: RawUtf8;
      aThreadPoolCount: Integer = 32;
      const aQueueName: SynUnicode = '';
      aHeadersUnFiltered: boolean = false); reintroduce; overload;
    /// release all memory, internal mORMot server and HTTP handlers
    destructor Destroy; override;
    procedure Shutdown(noRestServerShutdown: boolean = false);
    property HttpServer: TMqttServer read fHttpServer;
    property Port: RawUtf8 read fPort;
  end;

implementation

{ TRestHttpServer }

constructor TRestMqttServer.Create(const aPort: RawUtf8;
  aThreadPoolCount: Integer; const aQueueName: SynUnicode;
  aHeadersUnFiltered: boolean);
var
  i, j: PtrInt;
  ErrMsg: RawUtf8;
  log: ISynLog;
begin
  fLog := TSynLog;
  log := fLog.Enter('Create (NONE) on port %', [aPort], self);
  inherited Create;
  fPort := aPort;
  fHttpServer := TMqttServer.Create(fPort, HttpThreadStart,HttpThreadTerminate,'');
  fHttpServer.WaitStarted;
  log.Log(sllHttp, '% initialized for %', [fHttpServer, 'adb'], self);
end;

destructor TRestMqttServer.Destroy;
begin

  inherited;
end;

procedure TRestMqttServer.HttpThreadStart(Sender: TThread);
begin

end;

procedure TRestMqttServer.HttpThreadTerminate(Sender: TThread);
begin

end;

procedure TRestMqttServer.Shutdown(noRestServerShutdown: boolean);
begin

end;

end.

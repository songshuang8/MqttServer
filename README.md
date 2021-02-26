# MqttServer
 Based on mormots network,logs,sqlite,rest 
 
This set of units is still a work-in-progress
 
 how to use 
 in consel:
 
{$APPTYPE CONSOLE}

{$I mormot.defines.inc}

uses
  {$I mormot.uses.inc}
  SysUtils,
  MqttServer in 'src\MqttServer\MqttServer.pas',
  MqttUtils in 'src\MqttServer\MqttUtils.pas',
  mormot.net.async_rw in 'src\MqttServer\mormot.net.async_rw.pas',
  MqttApp in 'src\MqttServer\MqttApp.pas';

{$R *.res}

var
  server: TMQTTApp;
begin
  server := TMQTTApp.Create;
  try
    if server.Init then
    begin
      write('Server is now running on'#13#10#13#10+
        'Press [Enter] to quit');
      readln;
    end;
  finally
    Writeln('Start stoppeding...');
    FreeAndNil(server);
    Writeln('irrc stoppeded.');
  end;
end.
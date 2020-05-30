$service_version="5.0.1"
$service_name=$MyInvocation.MyCommand.Name.Split('_',2)[0]
$business_logic_name="MainBusinessLogic"
$datetimestamp=Get-Date -Format yyyyMMdd_HHmmss
$unique_service_id=$service_name+'_'+$service_version+'_'+$datetimestamp

$mstracing_library_path="C:\RabbitMQLib\Microsoft.Diagnostics.Tracing.EventSource.dll"
$rabbit_library_path="C:\RabbitMQLib\RabbitMQ.Client.dll"

$pass='guest'
$user='guest'
$server='rabbitmq01'
$port=5672
$exchange='main'
$queue=$business_logic_name,$service_name -join '.'
$wait_timeout=60
$hearbeat_timespan=New-TimeSpan -Seconds 300

$log_file=$unique_service_id+'.log'

((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Process started")  | Out-File $log_file -Append

try
{
   Add-Type -Path $mstracing_library_path
   Add-Type -Path $rabbit_library_path
}
catch
{
   ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Can't initialize RabbitMQ library")  | Out-File $log_file -Append
   "Message: $($_.Exception.Message)"| Out-File $log_file -Append
   "StackTrace: $($_.Exception.StackTrace)"| Out-File $log_file -Append
   "LoaderExceptions: $($_.Exception.LoaderExceptions)"| Out-File $log_file -Append
    Get-EventSubscriber -Force | Unregister-Event -Force
    if($Channel)
    {
        $Channel.Close()
    }
    if($Connection -and $Connection.IsOpen)
    {
        $Connection.Close()
    }
   Break
}

((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- RabbitMQ initialized")  | Out-File $log_file -Append


while ($true)
{
    try
    {
        $ConnectionFactory=New-Object RabbitMQ.Client.ConnectionFactory
        $ConnectionFactory.HostName=$server
        $ConnectionFactory.Port=$port
        $ConnectionFactory.UserName=$user
        $ConnectionFactory.Password=$pass
        $ConnectionFactory.AutomaticRecoveryEnabled=$false
        $ConnectionFactory.ClientProperties.version=(Get-ChildItem $rabbit_library_path).VersionInfo.FileVersion
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- RabbitMQ Connection Factory created to "+$ConnectionFactory.Endpoint)  | Out-File $log_file -Append
        $Connection = $ConnectionFactory.CreateConnection($unique_service_id)
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- RabbitMQ Connection created with ID "+$Connection.Id)  | Out-File $log_file -Append
        $Channel = $Connection.CreateModel()
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- RabbitMQ Channel created. "+$Channel.Session)  | Out-File $log_file -Append
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Subscribing to the RabbitMQ events for "+$queue+" on "+$server+"\"+$exchange)  | Out-File $log_file -Append
        $eventingBasicConsumer = New-Object RabbitMQ.Client.Events.EventingBasicConsumer($Channel)
        $sourceIdentifier=new-guid
        Register-ObjectEvent -InputObject $eventingBasicConsumer -EventName Received -SourceIdentifier $sourceIdentifier
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Starting consumer for events from "+$queue+" on "+$server+"\"+$exchange)  | Out-File $log_file -Append
        $Channel.BasicConsume($queue, $false,$unique_service_id,0,0,$null,$eventingBasicConsumer)
     }
    catch
    {
        ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Can't connect to RabbitMQ server")  | Out-File $log_file -Append
        "Message: $($_.Exception.Message)"| Out-File $log_file -Append
        "StackTrace: $($_.Exception.StackTrace)"| Out-File $log_file -Append
        "LoaderExceptions: $($_.Exception.LoaderExceptions)"| Out-File $log_file -Append
        Get-EventSubscriber -Force | Unregister-Event -Force
        if($Channel)
        {
            $Channel.Close()
        }
        if($Connection -and $Connection.IsOpen)
        {
            $Connection.Close()
        }
        Break
    }
    while ($Channel.IsOpen -and $Connection.IsOpen)
    {
        try
        {
            $error.Clear()
            ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Waiting for events from "+$queue+" on "+$server+"\"+$exchange)  | Out-File $log_file -Append
            $event=$null
            while ($Channel.IsOpen -and $Connection.IsOpen -and !$event)
            {
                if ((get-date) -ge $NextUpdate)
                {
                    ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Still waiting for events from "+$queue+" on "+$server+"\"+$exchange)  | Out-File $log_file -Append
                    $NextUpdate=(get-date)+$hearbeat_timespan
                }
                $event = Wait-Event -SourceIdentifier $sourceIdentifier -Timeout $wait_timeout
            }
            if (!$event)
            {
                ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Connection to RabbitMQ lost")  | Out-File $log_file -Append
                if ($Channel.IsClosed)
                {
                    ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Channel close reason: "+$Channel.CloseReason)  | Out-File $log_file -Append
                }
                if ($Connection.IsClosed)
                {
                    ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Connection close reason: "+$Connection.CloseReason)  | Out-File $log_file -Append
                }
                
                Get-EventSubscriber -Force | Unregister-Event -Force
                if($Channel)
                {
                    $Channel.Close()
                }
                if($Connection -and $Connection.IsOpen)
                {
                    $Connection.Close()
                }
                Break
            }
            $Message=$event.SourceEventArgs
            Remove-Event -EventIdentifier $event.EventIdentifier

            ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Received message")  | Out-File $log_file -Append
            $JSONRequest = [Text.Encoding]::UTF8.GetString($Message.Body) | ConvertFrom-Json
            $JSONRequest | Out-File $log_file -Append

            $htResponse = @{}

            $ResponseProps = $Channel.CreateBasicProperties()
            $ResponseProps.ContentType='application/json'

			$ResponseProps.timestamp=([DateTimeOffset](Get-Date)).ToUnixTimeSeconds()
			$ResponseProps.Headers=New-Object "System.Collections.Generic.Dictionary``2[System.String,System.Object]"
			$ResponseProps.Headers.Add('timestamp_net',(get-date -format o))

            $htResponse.SearchRequestId=$JSONRequest.RequestId
            $htResponse.AccountId=$JSONRequest.AccountId
            $htResponse.OCN=$JSONRequest.CarrierList[0]
            $htResponse.CarrierList=@{}
            if ($JSONRequest.CarrierList.Length -gt 1)
            {
                $htResponse.CarrierList=$JSONRequest.CarrierList[1..($JSONRequest.CarrierList.Length-1)]
            }

			if ($error.Count -eq 0)
			{
				$htResponse.Result='Success'
				$htResponse.Count=9999
			}
			else
			{
				$htResponse.Result='Failure'
				foreach ($err in $error) {$htResponse.Errors+=$err.Exception.Message}
			}

			$replyKey=$queue+'.'+$htResponse.Result
			$JSONResponse = (New-Object -TypeName PSObject -Property $htResponse) | ConvertTo-Json -Compress
			((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Prepared response to RabbitMQ: " + $JSONResponse)  | Out-File $log_file -Append
			((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Sending message to " + $replyKey + " on " + $server + "\" + $exchange)  | Out-File $log_file -Append
			$Channel.BasicPublish($exchange, $replyKey, $false, $ResponseProps, [System.Text.Encoding]::UTF8.GetBytes($JSONResponse))           
            $Channel.BasicAck($Message.DeliveryTag, $false)
        }
        catch
        {
            ((Get-Date -Format "yyyy/MM/dd HH:mm:ss.fff")+" --- Exception occured!")  | Out-File $log_file -Append
            "Message: $($_.Exception.Message)"| Out-File $log_file -Append
            "StackTrace: $($_.Exception.StackTrace)"| Out-File $log_file -Append
            "LoaderExceptions: $($_.Exception.LoaderExceptions)"| Out-File $log_file -Append
            Get-EventSubscriber -Force | Unregister-Event -Force
            if($Channel)
            {
                $Channel.Close()
            }
            if($Connection -and $Connection.IsOpen)
            {
                $Connection.Close()
            }
            Break
        }
    }
}        



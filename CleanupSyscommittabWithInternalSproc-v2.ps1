<#################################################################
Purpose: This script will run the official SQL sproc for cleaning
         up Syscommittab in order to cleanup Syscommittab. It tries
         to run the sproc until the sproc is actually triggered as
         it should be and performs the cleanup. If the cleanup is
         deadlocked it will retry up to 5 times.

Modification History:
06/15/2017   Benjamin Reynolds       Initial Creation
06/20/2017   Sherry Kissinger   Added logging to a file in the same folder as the script.
#################################################################>
Clear-Host
 
<#################################################################
    Variables (these need to be changed/updated)
#################################################################>
[int]$MaxIterations = 1800 # The number of times to try to cleanup before stopping the script (waits a second between trials); 1800 = 30 min (if all trials don't find a safe cleanup)
$VerboseLogging = $true # Control how much output you want to see
$SqlServer = "SCCMPS88" # The name of the server containing the database
$Database = "CM_A12" # The name of the database to check
$ConnectionTimeout = 120 # How long to wait for a connection timeout (in seconds)
$CommandTimeout = 28800 # How long to let the SQL script execute before stopping (in seconds); 28800 = 8 hours
$LogDirectory = Split-path -parent $PSCommandPath  #Place the log in the same folder this script exists when launched
$logfile = $LogDirectory + "\SqlSysCommitTabCleanup.log"
if (Test-Path $logfile) {
 Remove-Item $logfile
 }

<#################################################################
    Variables (aka Constants...don't change these)
#################################################################>
$SqlToExecute = "SET NOCOUNT OFF;
SET DEADLOCK_PRIORITY LOW;
EXECUTE sp_flush_commit_table_on_demand;";
$ConnectionString = "Server={0};Database={1};Integrated Security=SSPI;Connection Timeout={2}" -f $SqlServer,$Database,$ConnectionTimeout;
$Global:Output = @();
[int64]$Global:RowsAffected = $null;
#[int64]$CurRowsAffected = $null;
[int64]$safe_cleanup_version = 0;
[int]$NumberOfIterations = 0;
[int]$DeadlockedCount = 0;
$WasDeadlocked = $false;

<#################################################################
    Functions
#################################################################>

# This function is for logging

function log($String)
{
(Get-Date -format "dd-MM-yyy HH:mm:ss.mm") + "  " + $String | out-file -Filepath $logfile -append
}

##################################################################

# This function captures the sproc's output into global variables
function ProcessOutput {
    param ($event)
    
    if ($event.Message -ne $null) {
        $Global:Output += $event.Message
        #if ($VerboseLogging) {Write-Host $event.Message -ForegroundColor Green}
    }
    if ($event.RecordCount -gt 0) {
        $Global:Output += "RecordCount Captured Event = $($event.RecordCount)";
        #if ($VerboseLogging) {Write-Host $event.RecordCount -ForegroundColor Green}

        [int]$CurRowsAffected = $event.RecordCount
        $Global:RowsAffected += $CurRowsAffected
        if ($VerboseLogging) {Write-Host "Rows Deleted : $CurRowsAffected    ||    Total Rows Deleted so far: $Global:RowsAffected" -ForegroundColor Yellow;
                              log "Rows Deleted : $CurRowsAffected    ||    Total Rows Deleted so far: $Global:RowsAffected"}
    }
}
##################################################################

# This function makes the connection to SQL and tries to run the sproc
function CleanSyscommittab {
$Global:Output = @()

# Setup the SQL Connection:
$SqlConn = New-Object System.Data.SqlClient.SQLConnection
$SqlConn.ConnectionString = $ConnectionString
# Create an event handler since we need to capture the PRINT commands from the Sproc:
$EventHandler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender,$event) ProcessOutput $event}
$SqlConn.FireInfoMessageEventOnUserErrors = $true
$SqlConn.add_InfoMessage($EventHandler)
# Open the connection and create the command which we'll try in the try/catch:
$SqlConn.Open();
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand($SqlToExecute, $SqlConn); #$SqlConn.CreateCommand()
$SqlCmd.CommandText = $SqlToExecute
$SqlCmd.CommandTimeout = $CommandTimeout

$StmtCmpltdEventHndlr = [System.Data.StatementCompletedEventHandler] {param($sender,$event) ProcessOutput $event}
$SqlCmd.add_StatementCompleted($StmtCmpltdEventHndlr);





# Run the sproc (capturing the messages)
try {
    $Results = $SqlCmd.ExecuteNonQuery()
}
catch {
    Write-Host $_.Exception.Message -ForegroundColor Cyan
    log $_.Exception.Message
}
finally { # Make sure to close the connection whether successful or not
    $SqlConn.Close()
}
# Return the messages captured:
$Global:Output
}
##################################################################
##################################################################

<#################################################################
    Try to Cleanup
#################################################################>
# We'll keep trying until we get the real safe cleanup version
# unless we were deadlocked more than 5 times (this is handled
# inside the loop so probably not necessary here; but this is
# a failsafe just in case).
while ($safe_cleanup_version -eq 0 -and $DeadlockedCount -lt 6) {
    
    $NumberOfIterations += 1
    
    # if we've been trying and have reached the maximum number of trials then we'll stop
    if ($NumberOfIterations -gt $MaxIterations) {
        break
    }

    # Run the sproc:
    $CleanSyscommittabOutput = CleanSyscommittab
    
    <#
    # Determine the number of rows deleted:
    $RowsDeleted = $CleanSyscommittabOutput -like "(*row(s) affected)*"
    foreach ($Row in $RowsDeleted) {
        #if ($VerboseLogging) {Write-Host "Cur Row   : $Row"}
        $CurRowsAffected = $Row.Split(" ",2)[0].Substring(1)
        $RowsAffected += $CurRowsAffected
    }
    #>
    
    # Capture any deadlock information:
    if (($CleanSyscommittabOutput -like "Transaction*was deadlocked*").Count -gt 0) {
        $DeadlockedCount += 1;
        $WasDeadlocked = $true;
    }

    # Determine the safe cleanup version:
    $safe_cleanup_version = $CleanSyscommittabOutput[1].Substring(48,$CleanSyscommittabOutput[1].Length-49)

    # Verbose/Debugging output:
    if ($VerboseLogging) {
        $VerboseIterationLine = "Iteration $NumberOfIterations; Safe Cleanup Version = $safe_cleanup_version"
        if ($safe_cleanup_version -gt 0) {
            if ($WasDeadlocked) {
                $VerboseIterationLine += "; Rows Cleaned Up = $Global:RowsAffected; Trial was DEADLOCKED so may try again"
            }
            else {
                $VerboseIterationLine += "; Rows Cleaned Up = $Global:RowsAffected; script will stop now"
            }
        }
        else {
            $VerboseIterationLine += "; will retry in one second since safe cleanup version didn't return"
        }
        Write-Host $VerboseIterationLine;
        log $VerboseIterationLine
    }

    # If we were deadlocked (and haven't reached the 5 deadlock limit) reset the safe cleanup version so we try again:
    if ($WasDeadlocked -eq $true) {
        Write-Host "Iteration $NumberOfIterations found a safe cleanup version but was deadlocked" -ForegroundColor Cyan;
        log "Iteration $NumberOfIterations found a safe cleanup version but was deadlocked"

        if ($DeadlockedCount -le 5) {
            $safe_cleanup_version = 0;
            $WasDeadlocked = $false;
        }
        else {
            break;
        }
    }
    
    # Wait a second before retrying:
    Start-Sleep -Seconds 1;
}

<#################################################################
    Final Output
#################################################################>
Write-Host "";
log ""
if ($WasDeadlocked) {
    Write-Host "Script Ended with a deadlock!" -ForegroundColor Red;
    Write-Host "        Last Values      :";
    Write-Host "Safe Cleanup Version     : $safe_cleanup_version";
    Write-Host "Syscommittab Rows deleted: $Global:RowsAffected";
    Write-Host "Deadlocks Encountered    : $DeadlockedCount";
    Write-Host "Number of Iterations     : $NumberOfIterations";
    Write-Host "";
    Write-Host "*****************************************************";
    Write-Host "  Last Messages Captured :";

    log "Script Ended with a deadlock!"
    log "        Last Values      :"
    log "Safe Cleanup Version     : $safe_cleanup_version"
    log "Syscommittab Rows deleted: $Global:RowsAffected"
    log "Deadlocks Encountered    : $DeadlockedCount"
    log "Number of Iterations     : $NumberOfIterations"
    log "  Last Messages Captured :"

    foreach ($OutputLine in $CleanSyscommittabOutput) {
        Write-Host $OutputLine -ForegroundColor Cyan
        log $OutputLine
    }
    Write-Host "*****************************************************";
}
else {
    Write-Host "Script Completed!";
    Write-Host "        Last Values      :";
    Write-Host "Safe Cleanup Version     : $safe_cleanup_version";
    Write-Host "Syscommittab Rows deleted: $Global:RowsAffected";
    Write-Host "Deadlocks Encountered    : $DeadlockedCount";
    Write-Host "Number of Iterations     : $NumberOfIterations";
    
    log "Script Completed!"
    log "        Last Values      :"
    log "Safe Cleanup Version     : $safe_cleanup_version"
    log "Syscommittab Rows deleted: $Global:RowsAffected"
    log "Deadlocks Encountered    : $DeadlockedCount"
    log "Number of Iterations     : $NumberOfIterations"
    if ($VerboseLogging) {
        Write-Host "";
        Write-Host "*****************************************************";
        Write-Host "  Last Messages Captured :";
        foreach ($OutputLine in $CleanSyscommittabOutput) {
            Write-Host $OutputLine -ForegroundColor Cyan
        }
        Write-Host "*****************************************************";

        log "  Last Messages Captured :";
        foreach ($OutputLine in $CleanSyscommittabOutput) {
            log $OutputLine
        }
    }
}


##################################################################
##################################################################
##################################################################
<#
NOTES

-You can't see the progress of the sproc (when it actually starts to clean)
-If the sproc is deadlocked the rows affected isn't shown (at least from what I noticed); Here's the output of $CleanSyscommittabOutput (and $Global:Output)
 from a deadlock encounter. No rows affected were captured therefore it looks as if no rows were actually cleaned up...that could or could not be true since
 I can't tell when/where the deadlock was encountered.
   The value returned by change_tracking_hardened_cleanup_version() is 9585943047.
   The value returned by safe_cleanup_version() is 9585943047.
   Transaction (Process ID 237) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.
-Deadlocks aren't handled - so the script stops running and shows no records affected. Here's the output of the same run where a deadlock was encountered
 (the above output is what showed up after I decided to look at the variable to see if it had any more data). The output below does not say that any
 error was encountered but rather that no rows were deleted.
   Iteration 1; Safe Cleanup Version = 0; will retry since safe cleanup version didn't return
   Iteration 2; Safe Cleanup Version = 0; will retry since safe cleanup version didn't return
   ...
   Iteration 118; Safe Cleanup Version = 0; will retry since safe cleanup version didn't return
   Iteration 119; Safe Cleanup Version = 9585943047; Rows Cleaned Up = 0; script will stop now
   
   Script Ended. Last Values:
   Safe Cleanup Version     : 9585943047
   Syscommittab Rows deleted: 0
   Number of Iterations     : 118
#>
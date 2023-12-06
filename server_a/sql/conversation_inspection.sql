USE server_a_database
GO

SET ANSI_NULLS ON;
GO

-- Return a summary report of the service queue, queue readers, and active conversations
CREATE PROCEDURE usp_inspect_queue_status
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @near_service SYSNAME = N'server_a_service'
        , @queue_id INT
        , @max_queue_readers INT
        , @is_enqueue_enabled BIT
        , @is_activation_enabled BIT
        , @activation_procedure SYSNAME

        , @queue_monitor_state NVARCHAR(32)
        , @queue_status NVARCHAR(MAX)

        , @active_task_count INT
        , @active_conversation_count INT;

    SELECT @queue_id = sq.[object_id]
        , @max_queue_readers = sq.[max_readers]
        , @is_enqueue_enabled = sq.[is_enqueue_enabled]
        , @is_activation_enabled = sq.[is_activation_enabled]
        , @activation_procedure = sq.[activation_procedure]
    FROM sys.service_queues sq
    INNER JOIN sys.services s ON sq.[object_id] = s.[service_queue_id]
    WHERE s.[name] = @near_service;

    SET @queue_monitor_state = (
        SELECT [state]
        FROM sys.dm_broker_queue_monitors
        WHERE [queue_id] = @queue_id);

    DECLARE @active_tasks TABLE (
        [spid] INT);

    DECLARE @active_conversations TABLE (
        [conversation_id] UNIQUEIDENTIFIER);

    INSERT INTO @active_tasks ([spid])
        SELECT bat.[spid]
        FROM sys.dm_broker_activated_tasks bat
        INNER JOIN sys.dm_exec_requests er ON bat.[spid] = er.[session_id]
        WHERE bat.[queue_id] = @queue_id
            AND er.[wait_type] != N'BROKER_RECEIVE_WAITFOR'; -- filter out background queue readers

    INSERT INTO @active_conversations ([conversation_id])
        SELECT [conversation_id]
        FROM sys.conversation_endpoints ce
        INNER JOIN sys.services s ON ce.[service_id] = s.[service_id]
        WHERE s.[name] = @near_service
            AND ce.[state_desc] NOT IN ('CLOSED');

    SELECT @active_task_count = COUNT(*) FROM @active_tasks;
    SELECT @active_conversation_count = COUNT(*) FROM @active_conversations;

    SET @queue_status = CASE
        WHEN @queue_monitor_state = N'INACTIVE' 
            THEN N'The service queue is online and awaiting new messages.'
        WHEN @is_enqueue_enabled = 0 
            THEN N'The service queue is offline. Messages cannot be read, and additional messages cannot be enqueued.'
        WHEN @is_activation_enabled = 0 
            THEN N'The activation procedure for the service queue is disabled.'
        WHEN @activation_procedure IS NULL
            THEN N'There is no activation procedure specified for the service queue.' END

    IF @queue_status IS NOT NULL
        SELECT @queue_status [queue_status];
    ELSE
        SELECT CONCAT(
            'The service queue is currently reading ', @active_task_count,
                CASE @active_task_count WHEN 1 THEN ' message ' ELSE ' messages ' END, 'from ', @active_conversation_count,
                CASE @active_conversation_count WHEN 1 THEN ' active conversation. ' ELSE ' active conversations. ' END,
            'The service queue is configured to operate ', @max_queue_readers, ' queue ',
                CASE @max_queue_readers WHEN 1 THEN 'reader' ELSE 'readers' END, ' at a time.') [queue_status];
END
GO


-- Return a list of active conversations and their estimated time in the queue
CREATE PROCEDURE usp_inspect_active_conversations
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @near_service SYSNAME = N'server_a_service';

    DECLARE @active_conversations TABLE (
          [conversation_id] UNIQUEIDENTIFIER
        , [is_initiator] BIT
        , [state_desc] NVARCHAR(60)
        , [lifetime] DATETIME);

    INSERT INTO @active_conversations ([conversation_id], [is_initiator], [state_desc], [lifetime])
        SELECT [conversation_id], [is_initiator], [state_desc], [lifetime]
        FROM sys.conversation_endpoints ce
        INNER JOIN sys.services s ON ce.[service_id] = s.[service_id]
        WHERE s.[name] = @near_service
            AND [state_desc] NOT IN ('CLOSED');

    IF (SELECT COUNT(*) FROM @active_conversations) = 0
        SELECT 'The service queue contains no active conversations.' [active_conversations];
    ELSE
    BEGIN TRY

        -- MS Docs: "When no LIFETIME clause is specified, the dialog lifetime is the maximum value of the int data type"
        
        -- This fact is used to test whether an explicit dialog was set by the user,
        -- and to estimate the time a conversation has been active
        
        -- However, it does depend on users not setting a lifetime to a number near the max integer value,
        -- as such a behaviour would be redundant

        DECLARE @max_int INT = 2147483647;

        DECLARE @earliest_default_lifetime DATETIME = (
            SELECT DATEADD(SECOND, @max_int, [create_date]) -- creation date of broker service
            FROM sys.objects o
            INNER JOIN sys.services s ON o.object_id = s.service_queue_id
            WHERE s.[name] = @near_service);

        ;WITH
            lifetimes_for_comparison AS
            (
                SELECT [conversation_id]
                    , [lifetime]
                    , DATEADD(SECOND, @max_int, GETDATE()) [default_lifetime_from_now]
                FROM @active_conversations
            ),
            lifetimes_comparison AS
            (
                SELECT [conversation_id]
                    , [lifetime]
                    , DATEDIFF_BIG(MS, [lifetime], default_lifetime_from_now) [difference_in_ms]
                FROM lifetimes_for_comparison
            ),
            difference_as_date AS
            (
                SELECT [conversation_id]
                    , CASE WHEN [lifetime] > @earliest_default_lifetime 
                        THEN DATEADD(MS, difference_in_ms, 0)
                        ELSE NULL -- dialog lifetime used; force null through formatting
                        END [difference_as_date]
                FROM lifetimes_comparison
            ),
            date_formatting AS
            (
                SELECT [conversation_id]
                    , FORMAT(DATEPART(YEAR, difference_as_date) - 1900, '0000') [year_part]
                    , FORMAT(DATEPART(MONTH, difference_as_date) - 1, '00') [month_part]
                    , FORMAT(DATEPART(DAY, difference_as_date) - 1, '00') [day_part]
                    , FORMAT(difference_as_date, 'HH:mm:ss.fff') [time_part]
                FROM difference_as_date
            )
            ,date_display AS
            (
                SELECT [conversation_id]
                    , CASE WHEN time_part IS NOT NULL
                        THEN CONCAT(year_part, '-', month_part, '-', day_part, ' ', time_part)
                        ELSE 'An explicit dialog lifetime is set on this conversation; '
                            + 'its wait time cannot be estimated.'
                        END [estimated_time_in_queue]
                FROM date_formatting
            )
        SELECT c.[conversation_id] [sys_conversation_id]
            , [is_initiator]
            , [state_desc] [state_description]
            , estimated_time_in_queue
        FROM date_display d
        INNER JOIN @active_conversations c ON d.conversation_id = c.conversation_id
        ORDER BY estimated_time_in_queue DESC
    END TRY
    BEGIN CATCH
        SELECT 'The following error occurred while estimating the message wait times : '
            + ERROR_MESSAGE() [system_error];
    END CATCH
END
GO


-- Return a list of active queue readers 
-- with their current execution time and the name of the procedure that they are executing
CREATE PROCEDURE usp_inspect_queue_readers (
    @show_active_queue_readers BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @near_service SYSNAME = N'server_a_service'
        , @queue_id INT
        , @is_enqueue_enabled BIT
        , @is_activation_enabled BIT
        , @queue_monitor_state NVARCHAR(32);

    SELECT @queue_id = sq.[object_id]
        , @is_enqueue_enabled = sq.[is_enqueue_enabled]
        , @is_activation_enabled = sq.[is_activation_enabled]
    FROM sys.service_queues sq
    INNER JOIN sys.services s ON sq.[object_id] = s.[service_queue_id]
    WHERE s.[name] = @near_service;

    SET @queue_monitor_state = (
        SELECT [state]
        FROM sys.dm_broker_queue_monitors
        WHERE [queue_id] = @queue_id);

    DECLARE @active_tasks TABLE (
          [spid] INT
        , [command] NVARCHAR(32));

    INSERT INTO @active_tasks ([spid], [command])
        SELECT bat.[spid], er.[command]
        FROM sys.dm_broker_activated_tasks bat
        INNER JOIN sys.dm_exec_requests er ON bat.[spid] = er.[session_id]
        WHERE bat.[queue_id] = @queue_id
            AND er.[wait_type] != N'BROKER_RECEIVE_WAITFOR';

    IF @is_enqueue_enabled = 1 AND @is_activation_enabled = 1
    BEGIN TRY
        IF @queue_monitor_state = N'INACTIVE'
        BEGIN
            SELECT 'There are no active queue readers. There are no new messages to receive.' [queue_readers];
            RETURN;
        END

        -- Get the request text and the wait time of the currently executing queue readers,
        -- and extract the name of the procedure that they are executing
        ;WITH
            request_execution AS
            (
                SELECT act.[spid], t.[text], wt.[wait_duration_ms]
                FROM @active_tasks act
                INNER JOIN sys.dm_exec_requests er ON act.spid = er.session_id
                INNER JOIN sys.dm_os_waiting_tasks wt ON act.spid = wt.session_id
                CROSS APPLY sys.dm_exec_sql_text(er.[sql_handle]) t
            ),
            execution_time_as_date AS
            (
                SELECT [spid]
                    , DATEADD(MS, wait_duration_ms, 0) [duration_as_date]
                FROM request_execution
            ),
            date_conversion AS
            (
                SELECT [spid]
                    , FORMAT(DATEPART(YEAR, duration_as_date) - 1900, '0000') [year_part]
                    , FORMAT(DATEPART(MONTH, duration_as_date) - 1, '00') [month_part]
                    , FORMAT(DATEPART(DAY, duration_as_date) - 1, '00') [day_part]
                    , FORMAT(duration_as_date, 'HH:mm:ss.fff') [time_part]
                FROM execution_time_as_date
            ),
            date_display AS
            (
                SELECT [spid]
                    , CONCAT(year_part, '-', month_part, '-', day_part, ' ', time_part) [execution_time]
                FROM date_conversion
            ),
            proc_name_start AS
            (
                SELECT [spid], [text]
                    , CASE WHEN CHARINDEX('CREATE PROC ', [text]) > 0
                        THEN CHARINDEX('CREATE PROC ', [text]) + LEN('CREATE PROC ') + 1
                        ELSE CHARINDEX('CREATE PROCEDURE ', [text]) + LEN('CREATE PROCEDURE ') + 1 END [start_position]
                FROM request_execution
            ),
            proc_name_end AS -- find the first non-alphanumeric character after the start of the procedure name
            (
                SELECT [spid], [text], [start_position]
                    , PATINDEX('%[^a-zA-Z0-9_]%', SUBSTRING([text], start_position, LEN([text]))) [end_position] 
                FROM proc_name_start
            ),
            proc_name_check AS
            (
                SELECT [spid], [text]
                    , TRY_CONVERT(SYSNAME, SUBSTRING([text], start_position, end_position - 1)) [proc_name]
                FROM proc_name_end
            )
        SELECT p.[spid] [session_id]
            , CASE WHEN proc_name IS NOT NULL THEN proc_name
                ELSE 'The queue reader is executing a process that is not anticipated in this application.'
                END [executing_procedure_name]
            , execution_time [execution_time]
        FROM proc_name_check p
        INNER JOIN date_display d ON p.spid = d.spid
        ORDER BY execution_time DESC
    END TRY
    BEGIN CATCH
        SELECT 'The following error occurred while inspecting the queue readers: '
            + ERROR_MESSAGE() [system_error];
    END CATCH
END
GO
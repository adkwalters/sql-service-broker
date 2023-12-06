USE server_b_database
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON; -- required for XML methods
GO

-- Start a conversation by sending a request message using the input XML document
CREATE PROCEDURE usp_start_conversation (
      @message_payload XML
    , @conversation_log_id INT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @message_payload IS NULL
    BEGIN
        SELECT 'The input XML cannot be null.' [error];
        RETURN
    END

    BEGIN TRY
        DECLARE @near_service SYSNAME = N'server_b_service'
            , @far_service SYSNAME
            , @conversation_handle UNIQUEIDENTIFIER
            , @conversation_id UNIQUEIDENTIFIER
            , @message_send_time DATETIME
            , @message_xml XML;

        BEGIN TRANSACTION;

        SELECT @message_send_time = GETUTCDATE();
        
        -- Create request payload from the input xml
        SET @message_xml = (
            SELECT
            (
                SELECT 'request_message' [SendType]
                    , @message_send_time [SendTime]
                FOR XML PATH('MessageHeader'), TYPE
            )
            ,@message_payload [MessagePayload]
            FOR XML PATH('Message'), TYPE);

        BEGIN DIALOG CONVERSATION @conversation_handle
            FROM SERVICE @near_service
            TO SERVICE N'server_a_service'
            ON CONTRACT service_contract
            WITH
                -- LIFETIME = 10, -- uncomment to set a conversation lifetime (seconds)
                ENCRYPTION = OFF;

        SEND ON CONVERSATION @conversation_handle
            MESSAGE TYPE request_message (@message_xml);

        -- Uncomment to set a dialog timer (seconds)
        -- BEGIN CONVERSATION TIMER (@conversation_handle) TIMEOUT = 10;

        SELECT @far_service = [far_service]
            , @conversation_id = [conversation_id]
        FROM sys.conversation_endpoints
        WHERE [conversation_handle] = @conversation_handle;

        -- Log conversation
        BEGIN
            DECLARE @conv_log_id TABLE (id INT);
            INSERT INTO conversation_log (conversation_status, from_service, to_service, start_time)
            OUTPUT INSERTED.id INTO @conv_log_id
            VALUES (N'active', @near_service, @far_service, @message_send_time);

            INSERT INTO conversation_sys_reference (conversation_log_id, sys_conversation_id)
            SELECT id, @conversation_id
            FROM @conv_log_id;

            DECLARE @message_log_id INT;
            INSERT INTO message_log (conversation_log_id, message_type, is_incoming, send_time)
            SELECT id, N'request_message', 0, @message_send_time
            FROM @conv_log_id;

            INSERT INTO request_log (conversation_log_id, request_payload)
            SELECT id, @message_payload
            FROM @conv_log_id;
        END

        -- Select newly created conversation log ID
        SELECT @conversation_log_id = id
        FROM @conv_log_id;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        SELECT 'The following error occurred when starting the conversation: '
            + ERROR_MESSAGE() [system_error];
    END CATCH
END
GO


-- End the conversation with a user-defined error, after receiving and logging its contents
CREATE PROCEDURE usp_end_conversation (
      @input_conversation_id UNIQUEIDENTIFIER
    , @output_conversation_id UNIQUEIDENTIFIER OUTPUT
    , @output_cancellation_status NVARCHAR(MAX) OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @@NESTLEVEL < 2
    BEGIN
        SELECT 'This procedure cannot be executed directly. Please use ''usp_cancel_conversation''.' [user_error];
        RETURN;
    END

    DECLARE @near_service SYSNAME = N'server_b_service'
        , @far_service SYSNAME
        , @service_contract_name SYSNAME

        , @conversation_handle UNIQUEIDENTIFIER
        , @conversation_status NVARCHAR(50)

        , @message_receive_type SYSNAME
        , @message_receive_time DATETIME
        , @message_body VARBINARY(MAX)
        , @message_body_xml XML

        , @message_header_receive_type SYSNAME
        , @message_header_receive_time DATETIME
        , @message_header_send_type SYSNAME
        , @message_header_send_time DATETIME
        , @message_payload XML

        , @error_time DATETIME
        , @error_number INT
        , @error_message NVARCHAR(MAX)

        , @response_send_type SYSNAME
        , @response_message NVARCHAR(MAX)
        , @response_xml XML
        , @response_payload XML;

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT @conversation_handle = [conversation_handle]
            , @far_service = [far_service]
        FROM sys.conversation_endpoints
        WHERE [conversation_id] = @input_conversation_id;

        WAITFOR (
            RECEIVE TOP (1)
                  @message_receive_type = [message_type_name]
                , @message_body = [message_body]
            FROM server_b_service_queue
            WHERE conversation_handle = @conversation_handle
        ), TIMEOUT 1000;

        IF @@ROWCOUNT = 0
        BEGIN
            -- The message of this active conversation is not on the near service
            -- This means that the message must be enqueued on the far service

            -- It is therefore futile to send a user-defined error message; the far service will not receive it
            -- Before it could be received, the far server will error upon attempting to respond to the ended conversation

            END CONVERSATION @conversation_handle;

            SELECT @conversation_status = N'user cancelled'
                , @error_time = GETUTCDATE()
                , @error_number = 3617 -- query cancelled by user 
                , @error_message = N'The conversation was ended manually via the service '''
                    + @near_service + ''', before receiving a response.';
        END
        ELSE
        BEGIN 
            -- The message of this active conversation is on the near service
            -- End the conversation with a user-defined error

            SET @message_body_xml = CAST(@message_body AS XML);

            SELECT @message_header_receive_type = @message_body_xml.value('(Message/MessageHeader/ReceiveType)[1]', 'SYSNAME')
                , @message_header_receive_time = @message_body_xml.value('(Message/MessageHeader/ReceiveTime)[1]', 'DATETIME')
                , @message_header_send_type = @message_body_xml.value('(Message/MessageHeader/SendType)[1]', 'SYSNAME')
                , @message_header_send_time = @message_body_xml.value('(Message/MessageHeader/SendTime)[1]', 'DATETIME')
                , @message_payload = @message_body_xml.query('Message/MessagePayload/node()');

            SELECT @conversation_status = N'user cancelled'
                , @response_send_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
                , @error_time = GETUTCDATE()
                , @error_number = 3617 -- query cancelled by user
                , @error_message = N'The conversation was ended manually via the service '''
                    + @near_service + ''', while attempting to receive the message type '''
                    + @message_receive_type + '''.';

            SET @response_xml = (
                SELECT
                (
                    SELECT @message_receive_type [ReceiveType]
                        , @message_receive_time [ReceiveTime]
                        , @response_send_type [SendType]
                        , @error_time [SendTime]
                    FOR XML PATH('MessageHeader'), TYPE
                )
               ,(
                    SELECT @error_time [ErrorTime]
                        , @error_number [ErrorNumber]
                        , @error_message [ErrorMessage]
                    FOR XML PATH('MessagePayload'), TYPE
                )
                FOR XML PATH('Message'), TYPE);

            SET @response_message = (CAST(@response_xml AS NVARCHAR(MAX)));

            END CONVERSATION @conversation_handle
                WITH ERROR = @error_number DESCRIPTION = @response_message;
        END

        -- Log the event
        BEGIN
            -- Main conversation log
            BEGIN
                DECLARE @conv_log_id TABLE (id INT);

                UPDATE conversation_log
                SET conversation_status = @conversation_status, end_time = @error_time
                OUTPUT INSERTED.id INTO @conv_log_id
                FROM conversation_log l
                INNER JOIN conversation_sys_reference r ON l.id = r.conversation_log_id
                WHERE r.sys_conversation_id = @input_conversation_id;

                IF @@ROWCOUNT = 0
                    INSERT conversation_log (conversation_status, from_service, to_service, start_time, end_time)
                    OUTPUT INSERTED.id INTO @conv_log_id
                    VALUES (@conversation_status, @near_service, @far_service, @message_header_send_time, @error_time);
            END

            -- Global system reference
            INSERT INTO conversation_sys_reference (conversation_log_id, sys_conversation_id)
            SELECT id, @input_conversation_id
            FROM @conv_log_id
            WHERE NOT EXISTS (
                SELECT * FROM conversation_sys_reference
                WHERE sys_conversation_id = @input_conversation_id);

            -- Incoming message
            IF @message_receive_type IS NOT NULL
                INSERT INTO message_log (conversation_log_id, message_type, is_incoming, send_time, receive_time)
                SELECT id, @message_receive_type, 1, @message_header_send_time, @message_receive_time
                FROM @conv_log_id;

            -- Outgoing message
            IF @response_send_type IS NOT NULL
                INSERT INTO message_log (conversation_log_id, message_type, is_incoming, send_time)
                SELECT id, @response_send_type, 0, @error_time
                FROM @conv_log_id;

            -- Previous message
            IF @message_header_receive_type IS NOT NULL
                UPDATE message_log
                SET receive_time = @message_header_receive_time
                FROM message_log m
                INNER JOIN @conv_log_id l ON m.conversation_log_id = l.id
                WHERE message_type = @message_header_receive_type;

            SET @response_payload = (SELECT @conversation_status FOR XML PATH('Status'), TYPE);

            -- Update request payload (cancelled by initiator)
            IF @message_header_send_type IS NULL
                UPDATE request_log
                SET response_payload = @response_payload
                FROM request_log r
                INNER JOIN @conv_log_id l ON r.conversation_log_id = l.id;

            -- Request payload
            IF @message_header_send_type = N'request_message'
                INSERT request_log (conversation_log_id, request_payload, response_payload)
                SELECT id, @message_payload, @response_payload
                FROM @conv_log_id;

            -- Response payload
            IF @message_header_send_type = N'response_message'
                UPDATE request_log
                SET response_payload = @response_payload
                FROM request_log r
                INNER JOIN @conv_log_id l ON r.conversation_log_id = l.id;

            -- Error details
            INSERT INTO error_log (conversation_log_id, error_time, error_number, error_message)
            SELECT id, @error_time, @error_number, @error_message
            FROM @conv_log_id;
            
            -- Test: replace the above query with the following, to cause half the conversations to fail
            -- INSERT INTO error_log (conversation_log_id, error_time, error_number, error_message)
            -- SELECT CASE WHEN id % 2 = 0 THEN 1 ELSE id END
            --     , @error_time, @error_number, @error_message
            -- FROM @conv_log_id;
        END

        COMMIT TRANSACTION;
        SELECT @output_conversation_id = @input_conversation_id;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        SELECT @output_conversation_id = NULL, @output_cancellation_status = ERROR_MESSAGE();
    END CATCH
END
GO


-- Cancel all active conversations,
-- unless a conversation is specified, in which case cancel only one
CREATE PROCEDURE usp_cancel_conversations (
    @sys_conversation_id UNIQUEIDENTIFIER = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @near_service SYSNAME =  N'server_b_service'
        , @queue_id INT
        , @is_receive_enabled BIT
        , @is_activation_enabled BIT
        , @is_poison_message_handling_enabled BIT

        , @process_id SMALLINT
        , @rollback_process NVARCHAR(20)

        , @input_conversation_id UNIQUEIDENTIFIER
        , @output_conversation_id UNIQUEIDENTIFIER
        , @output_cancellation_status NVARCHAR(MAX);

    DECLARE @cancellation_status TABLE (
          [conversation_id] UNIQUEIDENTIFIER
        , [cancellation_status] NVARCHAR(MAX));

    SET @output_cancellation_status = NULL;

    SELECT @queue_id = sq.[object_id]
        , @is_receive_enabled = sq.[is_receive_enabled]
        , @is_activation_enabled = sq.[is_activation_enabled]
        , @is_poison_message_handling_enabled = sq.[is_poison_message_handling_enabled]
    FROM sys.service_queues sq
    INNER JOIN sys.services s ON sq.[object_id] = s.[service_queue_id]
    WHERE s.[name] = @near_service;

    -- User handling
    BEGIN
        IF @is_poison_message_handling_enabled = 0
        BEGIN
            SELECT 'Poison message handling is not enabled on the service queue. '
                + 'Active procedures cannot be terminated this way.' [user_error];
            RETURN;
        END

        IF NOT EXISTS (
            SELECT ce.[conversation_id]
            FROM sys.conversation_endpoints ce
            INNER JOIN sys.services s ON ce.[service_id] = s.[service_id]
            WHERE s.[name] = @near_service
                AND ce.[state_desc] NOT IN ('CLOSED')
                AND (ce.[conversation_id] = @sys_conversation_id OR @sys_conversation_id IS NULL))
        BEGIN
            SELECT CASE WHEN @sys_conversation_id IS NULL
                THEN 'The service queue contains no active conversations.'
                ELSE 'No conversation exists with the specified conversation_id.'
                END [user_error];
            RETURN;
        END
    END

    -- MS Docs: "If [a RECEIVE statement is rolled back] five times, 
    -- automatic poison message detection will set the queue status to OFF"

    -- This fact is used to stop all queue readers on the near service,
    -- by rolling back the queue readers as they're re-activated, until PM detection is triggered

    BEGIN TRY
        SET @process_id = (
            SELECT TOP 1 [spid]
            FROM sys.dm_broker_activated_tasks
            WHERE [queue_id] = @queue_id);

        WHILE @process_id IS NOT NULL
        BEGIN
            SET @rollback_process = N'KILL ' + CAST(@process_id AS NVARCHAR(5));

            EXEC sp_executesql @rollback_process;

            WAITFOR DELAY '00:00:00.5'; -- allow the queue monitor to reactive a queue reader

            SET @process_id = (
                SELECT TOP 1 [spid]
                FROM sys.dm_broker_activated_tasks
                WHERE [queue_id] = @queue_id);
        END
    END TRY
    BEGIN CATCH
        SELECT 'The following error occurred while killing active tasks: '
            + ERROR_MESSAGE() [system_error];
    END CATCH
    
    -- A service queue must be active in order to receive its messages
    -- Its queue readers must be inactve in order to allow the application control over which messages are read
    ALTER QUEUE server_b_service_queue
        WITH STATUS = ON, ACTIVATION (STATUS = OFF);

    -- Loop through the specified conversations and end them gracefully
    DECLARE conversation_cursor CURSOR FOR
        SELECT [conversation_id]
        FROM sys.conversation_endpoints ce
        INNER JOIN sys.services s ON ce.[service_id] = s.[service_id]
        WHERE s.[name] = @near_service
            AND ce.[state_desc] NOT IN ('CLOSED')
            AND (ce.[conversation_id] = @sys_conversation_id OR @sys_conversation_id IS NULL);

    OPEN conversation_cursor;
    FETCH NEXT FROM conversation_cursor INTO @input_conversation_id;
    WHILE @@FETCH_STATUS = 0
    BEGIN TRY
        EXEC usp_end_conversation 
              @input_conversation_id = @input_conversation_id
            , @output_conversation_id = @output_conversation_id OUTPUT
            , @output_cancellation_status = @output_cancellation_status OUTPUT;

        INSERT INTO @cancellation_status ([conversation_id], [cancellation_status])
        VALUES (@input_conversation_id, @output_cancellation_status);

        FETCH NEXT FROM conversation_cursor INTO @input_conversation_id;
    END TRY
    BEGIN CATCH
        SELECT 'The following error occurred while executing the user procedure ''usp_end_conversation'': '
            + ERROR_MESSAGE() [system_error];

        FETCH NEXT FROM conversation_cursor INTO @input_conversation_id;
    END CATCH
    CLOSE conversation_cursor;
    DEALLOCATE conversation_cursor;

    -- Reconfigure the queue as it was found
    IF @is_receive_enabled = 0
        ALTER QUEUE server_b_service_queue WITH STATUS = OFF;
    IF @is_activation_enabled = 1
        ALTER QUEUE server_b_service_queue WITH ACTIVATION (STATUS = ON);


    -- Output the result of the cancellations
    IF NOT EXISTS (SELECT 1 FROM @cancellation_status)
    BEGIN
        SELECT 'An unanticipated error ocurred. '
            + 'Please inspect the status of the service queue and try again.' [system_error];
        RETURN;
    END

    IF @@NESTLEVEL < 2 -- only return an output when the procedure is executed directly
        SELECT l.id [conversation_log_id]
            , r.sys_conversation_id 
            , CASE WHEN cs.cancellation_status IS NULL 
                THEN 'Conversation successfully cancelled'
                ELSE 'Conversation could not be cancelled: ' + cs.cancellation_status 
                END [cancellation_status]
        FROM @cancellation_status cs
        INNER JOIN conversation_sys_reference r ON cs.conversation_id = r.sys_conversation_id
        INNER JOIN conversation_log l ON r.conversation_log_id = l.id;
END
GO
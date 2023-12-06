USE server_a_database
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON; -- required for XML methods
GO

-- This procedure is internally activated by the service broker
-- A queue monitor checks whether activation is necessary every few seconds
CREATE PROCEDURE usp_activation_procedure
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF; -- allow post-error cleanup

    DECLARE @near_service SYSNAME
        , @far_service SYSNAME
        , @service_contract_name SYSNAME

        , @conversation_id UNIQUEIDENTIFIER
        , @conversation_handle UNIQUEIDENTIFIER
        , @conversation_status NVARCHAR(50)
        , @conversation_end_time DATETIME

        , @message_enqueue_time DATETIME
        , @message_receive_time DATETIME
        , @message_type SYSNAME
        , @message_body VARBINARY(MAX)
        , @message_body_xml XML

        , @message_header_receive_time DATETIME
        , @message_header_receive_type SYSNAME
        , @message_header_send_time DATETIME
        , @message_header_send_type SYSNAME
        , @message_payload XML

        , @sys_error_code INT
        , @sys_error_description NVARCHAR(MAX)

        , @error_xml XML
        , @error_time DATETIME
        , @error_xact_state SMALLINT
        , @error_number INT
        , @error_message NVARCHAR(MAX)

        , @response_xml XML
        , @response_send_time DATETIME
        , @response_send_type SYSNAME
        , @response_message NVARCHAR(MAX);
    
    BEGIN TRANSACTION;

    -- Receive message
    WAITFOR (
        RECEIVE TOP (1)
             @near_service = [service_name]
           , @conversation_handle = [conversation_handle]
           , @service_contract_name = [service_contract_name]
           , @message_enqueue_time = [message_enqueue_time]
           , @message_type = [message_type_name]
           , @message_body = [message_body]
        FROM server_a_service_queue
    ), TIMEOUT 1000;

    IF @@ROWCOUNT = 0
    BEGIN
        ROLLBACK TRANSACTION;
        RETURN;
    END

    SET @message_receive_time = GETUTCDATE();

    SELECT @far_service = [far_service]
        , @conversation_id = [conversation_id]
    FROM sys.conversation_endpoints
    WHERE [conversation_handle] = @conversation_handle;

    -- Filter out background queue readers
    IF @conversation_id IS NULL
    BEGIN 
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- Deserialise the message
    SET @message_body_xml = CAST(@message_body AS XML);
    SELECT @message_header_receive_time = @message_body_xml.value('(Message/MessageHeader/ReceiveTime)[1]', 'DATETIME')
        , @message_header_receive_type = @message_body_xml.value('(Message/MessageHeader/ReceiveType)[1]', 'SYSNAME')
        , @message_header_send_time = @message_body_xml.value('(Message/MessageHeader/SendTime)[1]', 'DATETIME')
        , @message_header_send_type = @message_body_xml.value('(Message/MessageHeader/SendType)[1]', 'SYSNAME')
        , @message_payload = @message_body_xml.query('Message/MessagePayload/node()');

    -- Route the message based on its message type name:
    --      System messages: EndDialog, DialogTimer, Error
    --      User messages: request_message, response_message

    IF @message_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
    BEGIN
        END CONVERSATION @conversation_handle;

        SELECT @conversation_status = N'complete'
            , @conversation_end_time = GETUTCDATE();
    END

    IF @message_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer'
    BEGIN
        END CONVERSATION @conversation_handle;

        SET @message_header_send_time = @message_enqueue_time-- DialogTimer messages are enqueued by the near service
        SELECT @conversation_status = N'timeout'
            , @conversation_end_time = GETUTCDATE()
            , @response_send_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
            , @error_time = @conversation_end_time
            , @error_number = 3617 -- query cancelled by user
            , @error_message = N'The conversation was ended after receiving a dialog timer.';
    END

    ELSE IF @message_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
    BEGIN
        -- Deserialise the error message
        WITH XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/SQL/ServiceBroker/Error')
            SELECT @sys_error_code = @message_body_xml.value('(/Error/Code)[1]', 'INT')
                , @sys_error_description = @message_body_xml.value('(Error/Description)[1]', 'NVARCHAR(4000)');

        IF @sys_error_code < 0 -- system error
        BEGIN
            SET @message_header_send_time = @message_enqueue_time -- DialogTimer messages are enqueued by the near service
            SELECT @conversation_status = N'system error'
                , @error_time = @message_enqueue_time 
                , @error_number = @sys_error_code
                , @error_message = @sys_error_description;
        END
        ELSE -- user-defined error
        BEGIN
            SET @error_xml = CAST(@sys_error_description AS XML);
            SELECT @message_header_receive_time = @error_xml.value('(Message/MessageHeader/ReceiveTime)[1]', 'DATETIME')
                , @message_header_receive_type = @error_xml.value('(Message/MessageHeader/ReceiveType)[1]', 'SYSNAME')
                , @message_header_send_time = @error_xml.value('(Message/MessageHeader/SendTime)[1]', 'DATETIME')
                , @error_time = @error_xml.value('(Message/MessagePayload/ErrorTime)[1]', 'DATETIME')
                , @error_number = @error_xml.value('(Message/MessagePayload/ErrorNumber)[1]', 'INT')
                , @error_message = @error_xml.value('(Message/MessagePayload/ErrorMessage)[1]', 'NVARCHAR(4000)');
        END

        END CONVERSATION @conversation_handle;

        SELECT @conversation_end_time = GETUTCDATE()
            , @conversation_status = CASE @error_number
                WHEN 3617 THEN N'user cancelled'
                WHEN -8489 THEN N'timeout'
                ELSE N'user error' END;
    END
    
    ELSE IF @message_type = N'request_message'
    BEGIN
        DECLARE @try_count SMALLINT = 0;
        WHILE (1=1) -- loop until broken
        BEGIN
            BEGIN TRY
                SET @try_count += 1;

                -- Execute the request payload
                DECLARE @xml_output XML
                EXEC usp_execute_sql_from_xml
                      @xml_in = @message_payload
                    , @xml_out = @xml_output OUTPUT;

                IF @xml_output IS NULL
                    SET @xml_output = (
                        SELECT N'complete'
                        FOR XML PATH('Status'));

                SELECT @conversation_status = N'active'
                    , @response_send_type = N'response_message'
                    , @response_send_time = GETUTCDATE();
            
                -- Create response payload
                SET @response_xml = (
                    SELECT
                    (
                        SELECT @message_type [ReceiveType]
                            , @message_receive_time [ReceiveTime]
                            , @response_send_type [SendType]
                            , @response_send_time [SendTime]
                        FOR XML PATH('MessageHeader'), TYPE
                    )
                   ,(
                        SELECT @xml_output
                        FOR XML PATH('MessagePayload'), TYPE
                    )
                    FOR XML PATH('Message'), TYPE);

                SEND ON CONVERSATION @conversation_handle
                    MESSAGE TYPE response_message (@response_xml);

                BREAK;
            END TRY
            BEGIN CATCH
                SELECT @error_xact_state = XACT_STATE()
                    , @error_number = ERROR_NUMBER()
                    , @error_message = ERROR_MESSAGE();

                IF @error_number = 8429 -- The conversation endpoint is not in a valid state for SEND
                BEGIN
                    ROLLBACK TRANSACTION;

                    END CONVERSATION @conversation_handle;

                    SELECT @error_time = GETUTCDATE()
                        , @conversation_status = N'disconnected'
                        , @conversation_end_time = @error_time
                        , @response_send_type = NULL;

                    BREAK;
                END
                
                IF @error_xact_state = -1 OR @try_count > 3
                BEGIN
                    ROLLBACK TRANSACTION;

                    SELECT @error_time = GETUTCDATE()
                        , @conversation_status = N'user error'
                        , @conversation_end_time = @error_time
                        , @response_send_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error';

                    -- Create user-defined error payload
                    SET @error_xml = (
                        SELECT
                        (
                            SELECT @message_type [ReceiveType]
                                , @message_receive_time [ReceiveTime]
                                , @response_send_type [SendType]
                                , @conversation_end_time [SendTime]
                            FOR XML PATH('MessageHeader'), TYPE
                        )
                       ,(
                            SELECT @error_time [ErrorTime]
                                , @error_number [ErrorNumber]
                                , @error_message [ErrorMessage]
                            FOR XML PATH('MessagePayload'), TYPE
                        )
                        FOR XML PATH('Message'), TYPE);

                    SET @response_message = (CAST(@error_xml AS NVARCHAR(4000)));
                
                    END CONVERSATION @conversation_handle
                        WITH ERROR = @error_number DESCRIPTION = @response_message;

                    BREAK;
                END
                ELSE
                    -- One second grace period for committable errors to resolve themselves
                    WAITFOR DELAY '00:00:01';
            END CATCH
        END
    END

    ELSE IF @message_type = N'response_message'
    BEGIN
        END CONVERSATION @conversation_handle;

        SELECT @conversation_status = N'complete'
            , @conversation_end_time = GETUTCDATE()
            , @response_send_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog';
    END

    -- Log the message and conversation
    BEGIN TRY
        -- Main conversation log
        BEGIN
            DECLARE @conv_log_id TABLE (id INT);

            UPDATE conversation_log
            SET conversation_status = @conversation_status
                , end_time = @conversation_end_time
            OUTPUT INSERTED.id INTO @conv_log_id
            FROM conversation_log l
            INNER JOIN conversation_sys_reference r ON l.id = r.conversation_log_id
            WHERE r.sys_conversation_id = @conversation_id;

            IF @@ROWCOUNT = 0
                INSERT conversation_log (conversation_status, from_service, to_service, start_time, end_time)
                OUTPUT INSERTED.id INTO @conv_log_id
                VALUES (@conversation_status, @far_service, @near_service, @message_header_send_time, @conversation_end_time);
        END

        -- Global system reference
        INSERT INTO conversation_sys_reference (conversation_log_id, sys_conversation_id)
        SELECT id, @conversation_id
        FROM @conv_log_id
        WHERE NOT EXISTS (
            SELECT * FROM conversation_sys_reference
            WHERE sys_conversation_id = @conversation_id);

        -- Incoming message
        INSERT INTO message_log (conversation_log_id, message_type, is_incoming, send_time, receive_time)
        SELECT id, @message_type, 1, @message_header_send_time, @message_receive_time
        FROM @conv_log_id;

        -- Outgoing message
        IF @response_send_type IS NOT NULL
        BEGIN
            SET @response_send_time = ISNULL(@conversation_end_time, @response_send_time);

            INSERT INTO message_log (conversation_log_id, message_type, is_incoming, send_time)
            SELECT id, @response_send_type, 0, @response_send_time
            FROM @conv_log_id;
        END

        -- Previous message
        IF @message_header_receive_type IS NOT NULL
            UPDATE message_log
            SET receive_time = @message_header_receive_time
            FROM message_log m
            INNER JOIN conversation_log c ON c.id = m.conversation_log_id
            INNER JOIN @conv_log_id l ON c.id = l.id
            WHERE message_type = @message_header_receive_type;

        -- Request payload
        IF @message_type = N'request_message'
            INSERT request_log (conversation_log_id, request_payload, response_payload)
            SELECT id, @message_payload, @xml_output
            FROM @conv_log_id;

        -- Response payload
        IF @message_type = N'response_message'
            UPDATE request_log
            SET response_payload = @message_payload
            FROM request_log r
            INNER JOIN @conv_log_id l ON r.conversation_log_id = l.id;

        IF @error_number IS NOT NULL
        BEGIN
            -- Mark response payload with error status
            UPDATE request_log
            SET response_payload = (
                SELECT @conversation_status
                FOR XML PATH('Status'), TYPE)
            FROM request_log r
            INNER JOIN @conv_log_id c ON r.conversation_log_id = c.id;

            -- Error details
            INSERT INTO error_log (conversation_log_id, error_time, error_number, error_message)
            SELECT id, @error_time, @error_number, @error_message
            FROM @conv_log_id;
        END
    END TRY
    BEGIN CATCH
        DECLARE @log_error_severity INT = ERROR_SEVERITY()
            , @log_error_xact_state SMALLINT = XACT_STATE()
            , @log_error_message NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @logging_error NVARCHAR(MAX) = CONCAT(
            'The following error occurred while logging a message of type ''', @message_type,
            ''' on conversation ', CAST(@conversation_id AS NVARCHAR(36)), ': ', @log_error_message);

        RAISERROR(@logging_error, @log_error_severity, @log_error_xact_state);
    END CATCH
    
    -- Only commit open transactions (errors are rolled back)
    IF @@TRANCOUNT = 1
        COMMIT TRANSACTION;
END
GO
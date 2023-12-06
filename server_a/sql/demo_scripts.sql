USE server_a_database
GO

-- The following procedures are used to abstract and order the execution of the demo procedures

-- To this end, each script waits for a response between each request, to guarantee the order of conversations
-- While the order of messages in a conversation is guaranteed by the service broker,
-- the order of conversations themselves is not.

SET ANSI_NULLS ON;
GO

-- Only allow demo scripts to be run when there are no active conversations,
-- as exceeding the number of queue readers will cause unexpected wait times
CREATE PROCEDURE usp_script_all_clear_for_demo
AS
BEGIN
    DECLARE @check_expiry_time DATETIME = DATEADD(SECOND, 2, GETDATE());

    WHILE EXISTS (
            SELECT TOP 1 conversation_id FROM sys.conversation_endpoints ce
            INNER JOIN sys.services s ON ce.[service_id] = s.[service_id]
            WHERE s.[name] = N'server_a_service'
                AND ce.[state_desc] NOT IN ('CLOSED'))
        IF GETDATE() > @check_expiry_time
            RETURN 1
    RETURN
END
GO


-- Demonstrate a simple use of this application
-- Complete four conversations in good order:
--      drop a table, create the table, insert a value, and then read from the table
-- Finally, output the final response from the read request
CREATE PROCEDURE usp_script_good_order
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @all_clear_check INT
    EXEC @all_clear_check = usp_script_all_clear_for_demo;
    IF @all_clear_check = 1
    BEGIN
        SELECT 'Please wait for all conversations to complete before running a demo script.' [error];
        RETURN
    END

    DECLARE @demo_drop_id INT
            , @demo_create_id INT
            , @demo_insert_id INT
            , @demo_read_id INT;
    
    DECLARE @procedure_expiry_time DATETIME = DATEADD(SECOND, 5, GETDATE());

    -- Get the drop proc as xml
    DECLARE @demo_drop_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
        , @xml_output = @demo_drop_output_xml OUTPUT;
    -- Send the drop proc in a request
    EXEC usp_start_conversation @message_payload = @demo_drop_output_xml
        , @conversation_log_id = @demo_drop_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_drop_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_drop_id);
    WHILE @demo_drop_response IS NULL
    BEGIN     
        SET @demo_drop_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_drop_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the create proc as xml
    DECLARE @demo_create_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
        , @xml_output = @demo_create_output_xml OUTPUT;
    -- Send the create proc in a request
    EXEC usp_start_conversation @message_payload = @demo_create_output_xml
        , @conversation_log_id = @demo_create_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_create_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_create_id);
    WHILE @demo_create_response IS NULL
    BEGIN     
        SET @demo_create_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_create_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the insert proc as xml
    DECLARE @demo_insert_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
        , @param_1 = N'@insert_string', @value_1 = 'Hello, World!'
        , @xml_output = @demo_insert_output_xml OUTPUT;
    -- Send the insert proc in a request
    EXEC usp_start_conversation @message_payload = @demo_insert_output_xml
        , @conversation_log_id = @demo_insert_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_insert_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_insert_id);
    WHILE @demo_insert_response IS NULL
    BEGIN     
        SET @demo_insert_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_insert_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the read proc as xml
    DECLARE @demo_read_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_read_table'
        , @return_xml = 1 -- request the output result set
        , @xml_output = @demo_read_output_xml OUTPUT;
    -- Send the read proc in a request
    EXEC usp_start_conversation @message_payload = @demo_read_output_xml
        , @conversation_log_id = @demo_read_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_read_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_read_id);
    WHILE @demo_read_response IS NULL
    BEGIN     
        SET @demo_read_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_read_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Read the result set from the response payload of the read request
    EXEC usp_show_conversation_response @conversation_log_id = @demo_read_id
END
GO


-- Demonstrate a simple misuse of this application
-- Complete four conversations in poor order:
--      drop a table, create the table, read from the table, and then insert a value
-- Finally, output the final response from the read request
CREATE PROCEDURE usp_script_bad_order
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @all_clear_check INT
    EXEC @all_clear_check = usp_script_all_clear_for_demo;
    IF @all_clear_check = 1
    BEGIN
        SELECT 'Please wait for all conversations to complete before running a demo script.' [error];
        RETURN
    END

    DECLARE @demo_drop_id INT
            , @demo_create_id INT
            , @demo_insert_id INT
            , @demo_read_id INT;

    DECLARE @procedure_expiry_time DATETIME = DATEADD(SECOND, 5, GETDATE());

    -- Get the drop proc as xml
    DECLARE @demo_drop_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
        , @xml_output = @demo_drop_output_xml OUTPUT;
    -- Send the drop proc in a request
    EXEC usp_start_conversation @message_payload = @demo_drop_output_xml
        , @conversation_log_id = @demo_drop_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_drop_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_drop_id);
    WHILE @demo_drop_response IS NULL
    BEGIN     
        SET @demo_drop_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_drop_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the create proc as xml
    DECLARE @demo_create_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
        , @xml_output = @demo_create_output_xml OUTPUT;
    -- Send the create proc in a request
    EXEC usp_start_conversation @message_payload = @demo_create_output_xml
        , @conversation_log_id = @demo_create_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_create_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_create_id);
    WHILE @demo_create_response IS NULL
    BEGIN     
        SET @demo_create_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_create_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the read proc as xml
    DECLARE @demo_read_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_read_table'
        , @return_xml = 1 -- request the output result set
        , @xml_output = @demo_read_output_xml OUTPUT;
    -- Send the read proc in a request
    EXEC usp_start_conversation @message_payload = @demo_read_output_xml
        , @conversation_log_id = @demo_read_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_read_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_read_id);
    WHILE @demo_read_response IS NULL
    BEGIN     
        SET @demo_read_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_read_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the insert proc as xml
    DECLARE @demo_insert_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
        , @param_1 = N'@insert_string', @value_1 = 'Hello, World!'
        , @xml_output = @demo_insert_output_xml OUTPUT;
    -- Send the insert proc in a request
    EXEC usp_start_conversation @message_payload = @demo_insert_output_xml
        , @conversation_log_id = @demo_insert_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_insert_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_insert_id);
    WHILE @demo_insert_response IS NULL
    BEGIN     
        SET @demo_insert_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_insert_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Read the result set from the response payload of the read request
    EXEC usp_show_conversation_response @conversation_log_id = @demo_read_id
END
GO


-- Demonstrate an advanced use of this application
-- Complete conversations to drop a table, create the table, and insert a value,
-- but before the insert request completes, cancel the conversation (as the initiator)
-- Finally, send a read request and output the final final response
CREATE PROCEDURE usp_script_cancellation
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @all_clear_check INT
    EXEC @all_clear_check = usp_script_all_clear_for_demo;
    IF @all_clear_check = 1
    BEGIN
        SELECT 'Please wait for all conversations to complete before running a demo script.' [error];
        RETURN
    END

    DECLARE @demo_drop_id INT
            , @demo_create_id INT
            , @demo_insert_id INT
            , @demo_cancel_id INT
            , @demo_read_id INT;

    DECLARE @procedure_expiry_time DATETIME = DATEADD(SECOND, 10, GETDATE());

    -- Get the drop proc as xml
    DECLARE @demo_drop_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
        , @xml_output = @demo_drop_output_xml OUTPUT;
    -- Send the drop proc in a request
    EXEC usp_start_conversation @message_payload = @demo_drop_output_xml
        , @conversation_log_id = @demo_drop_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_drop_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_drop_id);
    WHILE @demo_drop_response IS NULL
    BEGIN     
        SET @demo_drop_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_drop_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Get the create proc as xml
    DECLARE @demo_create_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
        , @xml_output = @demo_create_output_xml OUTPUT;
    -- Send the create proc in a request
    EXEC usp_start_conversation @message_payload = @demo_create_output_xml
        , @conversation_log_id = @demo_create_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_create_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_create_id);
    WHILE @demo_create_response IS NULL
    BEGIN     
        SET @demo_create_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_create_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Get the insert proc as xml
    DECLARE @demo_insert_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
        , @param_1 = N'@insert_string', @value_1 = 'Hello, World!'
        , @param_2 = N'@waitfor_delay', @value_2 = N'00:00:03'
        , @xml_output = @demo_insert_xml OUTPUT;
    -- Send the insert proc in a request
    EXEC usp_start_conversation @message_payload = @demo_insert_xml
        , @conversation_log_id = @demo_insert_id OUTPUT;
    -- Wait for insert message to be received BUT don't wait for a response
    WAITFOR DELAY '00:00:01';

    -- Cancel the conversation of the insert request (causes the insert to rollback)
    DECLARE @demo_insert_sys_conversation_id UNIQUEIDENTIFIER = (
        SELECT sys_conversation_id
        FROM conversation_sys_reference
        WHERE conversation_log_id = @demo_insert_id)
    EXEC usp_cancel_conversations @sys_conversation_id = @demo_insert_sys_conversation_id
    -- Wait for a response
    DECLARE @demo_insert_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_insert_id);
    WHILE @demo_insert_response IS NULL
    BEGIN     
        SET @demo_insert_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_insert_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Get the read proc as xml
    DECLARE @demo_read_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_read_table'
        , @return_xml = 1 -- request the output result set
        , @xml_output = @demo_read_output_xml OUTPUT;
    -- Send the read proc in a request
    EXEC usp_start_conversation @message_payload = @demo_read_output_xml
        , @conversation_log_id = @demo_read_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_read_response XML = (
        SELECT response_payload FROM request_log
        WHERE conversation_log_id = @demo_read_id);
    WHILE @demo_read_response IS NULL
    BEGIN     
        SET @demo_read_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_read_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Read the result set from the response payload of the read request
    EXEC usp_output_xml_as_table @xml_input = @demo_read_response;
END
GO


-- Demonstrate a simple misuse of this application
-- Complete two conversations in an order that causes an (xact-state 1) error:
--      drop a table and then insert a value into the table
-- Finally, output the error log from the failed insert
-- Note, xact-state 1 errors are retried 3 times, with a second delay between each try
CREATE PROCEDURE usp_script_error_retry
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @all_clear_check INT
    EXEC @all_clear_check = usp_script_all_clear_for_demo;
    IF @all_clear_check = 1
    BEGIN
        SELECT 'Please wait for all conversations to complete before running a demo script.' [error];
        RETURN
    END

    DECLARE @demo_drop_id INT
            , @demo_create_id INT
            , @demo_insert_id INT;

    DECLARE @procedure_expiry_time DATETIME = DATEADD(SECOND, 5, GETDATE());

    -- Get the drop proc as xml
    DECLARE @demo_drop_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
        , @xml_output = @demo_drop_output_xml OUTPUT;
    -- Send the drop proc in a request
    EXEC usp_start_conversation @message_payload = @demo_drop_output_xml
        , @conversation_log_id = @demo_drop_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_drop_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_drop_id);
    WHILE @demo_drop_response IS NULL
    BEGIN     
        SET @demo_drop_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_drop_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Note, the following insert request will cause xact-state 1
    -- The activation procedure will retry the command three time, with a second delay between each try
    -- This is to allow such errors to resolve by themselves, although here it is not, as the above wait precludes it

    -- Get the insert proc as xml
    DECLARE @demo_insert_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
        , @param_1 = N'@insert_string', @value_1 = 'Hello, World!'
        , @xml_output = @demo_insert_output_xml OUTPUT;
    -- Send the insert proc in a request
    EXEC usp_start_conversation @message_payload = @demo_insert_output_xml
        , @conversation_log_id = @demo_insert_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_insert_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_insert_id);
    WHILE @demo_insert_response IS NULL
    BEGIN     
        SET @demo_insert_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_insert_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Read the error log that results from the insert request
    SELECT error_message FROM error_log WHERE conversation_log_id = @demo_insert_id;
END
GO


-- Demonstrate a simple misuse of this application
-- Complete three conversations in an order that causes an (xact-state -1) error:
--      drop a table, create the table, then create the table again
-- Finally, output the error log from the failed insert
-- Note, xact-state -1 errors fail immediately without being retried
CREATE PROCEDURE usp_script_error_immediate
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @all_clear_check INT
    EXEC @all_clear_check = usp_script_all_clear_for_demo;
    IF @all_clear_check = 1
    BEGIN
        SELECT 'Please wait for all conversations to complete before running a demo script.' [error];
        RETURN
    END

    DECLARE @demo_drop_id INT
            , @demo_create_id INT
            , @demo_create_again_id INT;

    DECLARE @procedure_expiry_time DATETIME = DATEADD(SECOND, 5, GETDATE());

    -- Get the drop proc as xml
    DECLARE @demo_drop_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
        , @xml_output = @demo_drop_output_xml OUTPUT;
    -- Send the drop proc in a request
    EXEC usp_start_conversation @message_payload = @demo_drop_output_xml
        , @conversation_log_id = @demo_drop_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_drop_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_drop_id);
    WHILE @demo_drop_response IS NULL
    BEGIN     
        SET @demo_drop_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_drop_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 5 seconds.' [error];
            RETURN
        END
    END

    -- Get the create proc as xml
    DECLARE @demo_create_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
        , @xml_output = @demo_create_output_xml OUTPUT;
    -- Send the create proc in a request
    EXEC usp_start_conversation @message_payload = @demo_create_output_xml
        , @conversation_log_id = @demo_create_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_create_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_create_id);
    WHILE @demo_create_response IS NULL
    BEGIN     
        SET @demo_create_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_create_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Note, the following create request will cause xact-state -1 (uncommittable transaction)
    -- The activation procedure will not retry the command, and will fail immediately

    -- Get the create proc as xml
    DECLARE @demo_create_again_output_xml XML;
    EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
        , @xml_output = @demo_create_again_output_xml OUTPUT;
    -- Send the create proc in a request
    EXEC usp_start_conversation @message_payload = @demo_create_again_output_xml
        , @conversation_log_id = @demo_create_again_id OUTPUT;
    -- Wait for a response
    DECLARE @demo_create_again_response XML = (
        SELECT response_payload FROM request_log 
        WHERE conversation_log_id = @demo_create_again_id);
    WHILE @demo_create_again_response IS NULL
    BEGIN     
        SET @demo_create_again_response = (
            SELECT response_payload FROM request_log 
            WHERE conversation_log_id = @demo_create_again_id);
        IF GETDATE() > @procedure_expiry_time
        BEGIN
            SELECT 'The demonstration has unexpectedly taken longer than 10 seconds.' [error];
            RETURN
        END
    END

    -- Read the error log that results from the insert request
    SELECT error_message FROM error_log WHERE conversation_log_id = @demo_create_again_id;
END
GO
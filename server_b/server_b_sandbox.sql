USE server_b_database
GO

-- The following is intended as a sandbox for users to play with the app
-- See README.md for further explanation

-- --====== Demo Scripts =======--
-- The following scripts demonstrate simple use cases of this application
-- See README.md for an overview and demo_scripts.sql for details of each
EXEC usp_script_good_order -- returns inserted value
EXEC usp_script_bad_order -- returns empty set
EXEC usp_script_cancellation -- (long) returns empty set
EXEC usp_script_error_retry -- (long) returns error message
EXEC usp_script_error_immediate -- returns error message


-- --====== Demo Requests ======--
-- Each of the batches below remotely invoke prepared queries on the far service
-- See demo_procedures.sql for details of the prepared queries
-- See conversation_utils.sql for details of the query serialisation
-- See conversation_management.sql for details of the conversation initiation

-- Note, the broker is configured to read 5 messages at a time (see broker_objects.sql)
-- Additional messages will only be read once a queue reader becomes available

-- Drop table
BEGIN
   DECLARE @drop_demo_table XML, @drop_id INT;
   EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
      , @xml_output = @drop_demo_table OUTPUT;
   EXEC usp_start_conversation @message_payload = @drop_demo_table
      , @conversation_log_id = @drop_id;
END

-- Create table
BEGIN
   DECLARE @create_demo_table XML, @create_id INT;
   EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
      , @xml_output = @create_demo_table OUTPUT;
   EXEC usp_start_conversation @message_payload = @create_demo_table
      , @conversation_log_id = @create_id;
END

-- Insert table
BEGIN
   DECLARE @insert_demo_table XML, @insert_id INT;
   EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
      , @param_1 = N'@insert_string', @value_1 = 'Hello, World' -- insert a different string
      , @param_2 = N'@waitfor_delay', @value_2 = '00:01:00' -- increase the wait time to simulate a long-running process
      , @xml_output = @insert_demo_table OUTPUT;
   EXEC usp_start_conversation @message_payload = @insert_demo_table
      , @conversation_log_id = @insert_id;
END

-- Read table
BEGIN
   DECLARE @read_demo_table XML, @read_id INT;
   EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_read_table'
      , @return_xml = 1
      , @xml_output = @read_demo_table OUTPUT;
   EXEC usp_start_conversation @message_payload = @read_demo_table
      , @conversation_log_id = @read_id;
END

-- Simulate error
BEGIN
   DECLARE @simulate_error XML, @error_id INT;
   EXEC usp_output_sql_as_xml @procedure_name = N'usp_simulate_error'
      , @return_xml = 0
      , @xml_output = @simulate_error OUTPUT;
   EXEC usp_start_conversation @message_payload = @simulate_error
      , @conversation_log_id = @error_id;
END


-- --====== Read a Response ===--
-- Return the response payload from the last request that was sent by this service
-- See conversation_utils.sql for details of the response payload deserialisation
BEGIN
   DECLARE @last_log_id INT = (
      SELECT TOP 1 id
      FROM conversation_log 
      WHERE from_service = 'server_b_service' 
      ORDER BY id DESC);
   EXEC usp_show_conversation_response @conversation_log_id = @last_log_id;
END


-- --======= ! Cancel Conversations ! =======--
-- This procedure can be run to gracefully cancel all conversations or just one
-- Cancelling just one conversation requires the global sys_conversation_id, which can be retrieved from
--    the user table: conversation_sys_reference
--    the user procedure: usp_inspect_active_conversations

-- Note, it is only possible to immediately stop an active queue reader on the near service 
-- The far service will rollback its work once it tries (and fails) to respond to the ended conversation

-- Cancel all
EXEC usp_cancel_conversations;

-- Cancel the last request sent by this service
DECLARE @last_sys_id INT = (
   SELECT TOP 1 sys_conversation_id
   FROM conversation_sys_reference r
   INNER JOIN conversation_log l ON r.conversation_log_id = l.id
   WHERE from_service = 'server_b_service'
   ORDER BY id DESC);
EXEC usp_cancel_conversations @sys_conversation_id = @last_sys_id;


-- --====== Inspect the Broker =======--
-- The following procedures can be used to inspect the current status of the broker
-- See conversation_inspection.sql for details of each
EXEC usp_inspect_queue_status -- general health
EXEC usp_inspect_active_conversations -- active conversations
EXEC usp_inspect_queue_readers -- messages being read


-- --====== User Tables =======--
-- The following tables store conversation data
-- See logging_objects.sql for details and mapping
SELECT * FROM conversation_log
SELECT * FROM message_log
SELECT * FROM request_log
SELECT * FROM error_log
SELECT * FROM conversation_sys_reference


-- --====== System Tables =====--
-- The following system tables can help when things go wrong
SELECT * FROM server_b_service_queue WITH (NOLOCK)
SELECT * FROM sys.transmission_queue
SELECT * FROM sys.conversation_endpoints
SELECT * FROM sys.service_queues
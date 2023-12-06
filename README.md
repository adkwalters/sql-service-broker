# SQL Service Broker

Securely and reliably process database queries over separate server instances in a distributed OLTP system.

## Contents

- [Description](#description)
- [Dependencies](#dependencies)
- [Quick Start](#quick-start) <--
- [Demo Scripts](#demo-scripts)
- [Full Demonstration](#full-demonstration)
- [Durability Testing](#durability-testing)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)
- [Acknowledgements](#acknowledgements)

## Description

Send database queries and results between SQL Server instances using Service Broker conversations.

A conversation is initiated with a database request. The request is serialised into XML and sent in message to a far service, where it is deserialised and executed. The execution result is then serialised and sent back to the near service, where it can be deserialised and read in table form.

The application establishes a secure TCP/IP connection between the services using TLS authentication. The positive acknowledgement mechanism of TCP, along with the transaction control of the application, guarantees that conversation messages are processed exactly-one-in-order (EOIO), even in the face of total system failure ([see durability testing](#durability-testing)).

Furthermore, the application builds upon the dynamic management views and functions of the underlying broker to provide administrators with tools to inspect the current state of the broker and to cancel conversations while complying with ACID principles.

In order for the application to be available cross-platform, each server instance is run within a Docker container.

## Dependencies

- [Docker Desktop](https://docs.docker.com/desktop/) (v4.16 or later)
    -  For Apple Silicon machines, ensure [Rosetta emulation](https://devblogs.microsoft.com/azure-sql/wp-content/uploads/sites/56/2023/01/dockerdesktop-beta.png) is enabled

## Quick Start

Navigate to this repository using a CLI (CMD, Bash, Terminal), and run the following command:

Note, it can take a couple of minutes to download SQL Server, depending on your machine and its internet connection.
```
docker compose up -d
```

Once the app has finished initialising, start an interactive session on server_a:
```
docker exec -it server_a /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MSsql2023! -d server_a_database
```

Once the session has begun, run a [demo script](#demo-scripts) and see the result:
```
EXEC usp_script_good_order;
```
```
GO
```
To exit a session:
```
EXIT
```

## Demo scripts

Replace the demo script in the [quick start](#quick-start) with the ones below.

Complete four conversations in good order. Drop a table, create the table, insert a value, and then read from the table. Output the deserialised response from the read request showing the inserted value:
```
EXEC usp_script_good_order;
```

Complete four conversations in poor order. Drop a table, create the table, read from the table, and then insert a value. Output the deserialised response from the read request showing the empty table. Note, if the read request were sent again, the inserted value would be returned:
```
EXEC usp_script_bad_order;
```

(long) Complete conversations to drop a table, create the table, and insert a value, but before the insert request completes, cancel the conversation, then read from the table. Output the deserialised response from the read request showing the empty table after the insertion rollback:
```
EXEC usp_script_cancellation;
```

(long) Complete two conversations in an order that causes an (xact-state 1) error - drop a table and then insert a value into the table. Output the logged error. Note, xact-state 1 errors are retried 3 times, with a second delay between each try:
```
EXEC usp_script_error_retry;
```

Complete three conversations in an order that causes an (xact-state -1) error - drop a table, create the table, then create the table again. Output the logged error. Note, xact-state -1 errors fail immediately without being retried:
```
EXEC usp_script_error_immediate;
```

## Full Demonstration

Following on from the [quick start](#quick-start), the full demonstration comprises procedures to:
- [send requests](#send-requests)
- [read responses](#read-a-response)
- [cancel conversations](#cancel-conversations)
- [inspect the broker state](#inspect-the-broker-state)

All procedures can be run on both servers. To start an interactive session in a terminal:
- server_a
```
docker exec -it server_a /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MSsql2023! -d server_a_database
```
- server_b
```
docker exec -it server_b /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P MSsql2023! -d server_b_database
```

Alternatively, use the following connection strings with your chosen DBA tool:
- server_a
```
Server=localhost,1433;User Id=sa;Password=MSsql2023!;Database=server_a_database;TrustServerCertificate=True;
```
- server_b
```
Server=localhost,1434;User Id=sa;Password=MSsql2023!;Database=server_b_database;TrustServerCertificate=True;
```

All queries can be found in the ```playground.sql``` files in this repository.

### Send Requests 

Each of the [demo scripts](#demo-scripts) from the [quick start](#quick-start) use the following query batches. Each batch executes two procedures in order - the first to serialise the request and the second to send it as a conversation message.

Note, the broker is configured to operate 5 queue readers at a time. If all queue readers are active, additional messages will be enqueued on the far service but won't be read until a queue reader becomes available.

Drop the demo table (if it exists):
``` 
DECLARE @drop_demo_table XML, @drop_id INT;
EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_drop_table'
   , @xml_output = @drop_demo_table OUTPUT;
EXEC usp_start_conversation @message_payload = @drop_demo_table
   , @conversation_log_id = @drop_id;
```

Create the demo table:
```
DECLARE @create_demo_table XML, @create_id INT;
EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_create_table'
   , @xml_output = @create_demo_table OUTPUT;
EXEC usp_start_conversation @message_payload = @create_demo_table
   , @conversation_log_id = @create_id;
```

Insert a value into the demo table (the wait time simulates a long-running process):
```
DECLARE @insert_demo_table XML, @insert_id INT;
EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_insert_table'
   , @param_1 = N'@insert_string', @value_1 = 'Hello, World'
   , @param_2 = N'@waitfor_delay', @value_2 = '00:00:01'
   , @xml_output = @insert_demo_table OUTPUT;
EXEC usp_start_conversation @message_payload = @insert_demo_table
   , @conversation_log_id = @insert_id;
```

Read from the demo table:
```
DECLARE @read_demo_table XML, @read_id INT;
EXEC usp_output_sql_as_xml @procedure_name = N'usp_demo_read_table'
    , @return_xml = 1
    , @xml_output = @read_demo_table OUTPUT;
EXEC usp_start_conversation @message_payload = @read_demo_table
    , @conversation_log_id = @read_id;
```

Simulate an error:
```
DECLARE @simulate_error XML, @error_id INT;
EXEC usp_output_sql_as_xml @procedure_name = N'usp_simulate_error'
    , @return_xml = 0
    , @xml_output = @simulate_error OUTPUT;
EXEC usp_start_conversation @message_payload = @simulate_error
    , @conversation_log_id = @error_id;
```

### Read a Response
Once a conversation is complete, its response can be read from either server. If the response contains a result set, its xml is deserialised and read in table format:
```
EXEC usp_show_conversation_response @conversation_log_id = 
```

For example, from server_a, to get the response of the last request:
```
DECLARE @last_log_id INT = (
   SELECT TOP 1 id 
   FROM conversation_log 
   WHERE from_service = 'server_a_service' 
   ORDER BY id DESC);
EXEC usp_show_conversation_response @conversation_log_id = @last_log_id;
```

### Cancel Conversations
The following procedure can be run to immediately cancel and rollback all active conversations on the near service.

Note, it is not possible to immediately stop an active queue reader on a far service. It is only possible to end the conversation on the near service. The far service will rollback its work once it tries (and fails) to respond to the ended conversation.
```
EXEC usp_cancel_conversations;
```

To cancel a single, specific conversation, run the cancellation procedure with the GUID that identifies the conversation globally between all services involved in the conversation:
```
EXEC usp_cancel_conversations @sys_conversation_id =
```
For example, from server_a, to cancel the last request sent:
```
DECLARE @last_sys_id INT = (
   SELECT TOP 1 sys_conversation_id
   FROM conversation_sys_reference r
   INNER JOIN conversation_log l ON r.conversation_log_id = l.id
   WHERE from_service = 'server_a_service'
   ORDER BY id DESC);
EXEC usp_cancel_conversations @sys_conversation_id = @last_sys_id;
```

### Inspect the Broker State

Return a summary report of the service queue, its queue readers and active conversations:
```
EXEC usp_inspect_queue_status;
```

Return a list of active conversations and their estimated time spent in the queue:
```
EXEC usp_inspect_active_conversations;
```

Return a list of active queue readers on the near service, along with their current execution time and the name of the procedure that they are executing:
```
EXEC usp_inspect_queue_readers;
```

To test these procedures, spam the queue with requests. Increasing the wait time on the insert request will provide time to connect and inspect the far service.

## Durability Testing

The application guarantees that messages are properly processed in the face of total system failure, such as a power outage.

To test this assertion, submit a request to insert a value into the demo table. Then, stop both servers by killing their Docker containers without letting them shutdown gracefully:
```
docker kill server_a
docker kill server_b
```

After a few minutes (or days), re-start both containers:
```
docker start server_a
docker start server_b
```
When running the inspection procedures from the far service, the application will show the request message being re-processed. Note its execution time and its estimated time spent in the queue.


## Troubleshooting

### Login Problems

Depending on the host machine, after first initialisation, the application may take a few moments to properly respond to login. If the login issues persist longer than 30 seconds, the issue likely remains elsewhere. It can be helpful to check Docker Desktop to see the status and CPU load of each container.

Depending on the host network, connection to Docker containers may be refused. This may be due to firewall settings or port number conflicts. To check whether this is the case, try to establish a connection through the sqlcmd tool (see the [quick start](#quick-start)). The port numbers used by this application can be updated from the ```docker-compose.yml``` file.


## Limitations

### Message Logging

While the application logs all messages on all services, the send or receive time of some messages cannot be logged.

In fact, there are many send and receive times that are not available from the perspective of one service. For example, a service does not 'know' what time an incoming message was sent, only the time it is received. The application uses successive message payloads within a conversation to share this information between services.

However, a service can never know the time a far service receives its 'EndDialog' message, because the conversation has already ended. Similarly, the far service cannot know the time that the directly preceding message was received by the near service, as this data cannot be appended to the payload of the EndDialog message.

### Mapping Activated Tasks to Conversations

The [inspection procedures](#inspect-the-broker-state) of this application provide information about active conversations and queue readers. They do not, however, provide a direct relationship between the two. The queue object that would provide the data for this relationship is locked while messages are being read from it. This is by design of the underlying broker to ensure the isolation of transactions. 

To help solve this issue, the application seeks to provide sufficient data for the user to infer the mapping themselves. This includes the conversation's estimated time in the queue, the queue reader's execution time, and the name of the procedure that is being executed.

## Acknowledgements

Special thanks to [Remus Rusanu](https://rusanu.com/) and [SQLPassion](https://www.sqlpassion.at/), whose online content provided great insight into understanding the Service Broker and serving it through Docker, respectively.
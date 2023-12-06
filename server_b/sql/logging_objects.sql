USE master
GO

CREATE DATABASE server_b_database
GO

USE server_b_database
GO

-- The conversation_log is the main object for tracking conversations locally
-- To identify a conversation globally across all services, use sys_conversation_id

CREATE TABLE conversation_log (
    id INT IDENTITY(1,1) PRIMARY KEY
  , conversation_status NVARCHAR(50)
  , from_service SYSNAME NOT NULL
  , to_service SYSNAME NOT NULL
  , start_time DATETIME
  , end_time DATETIME);

CREATE TABLE conversation_sys_reference (
    conversation_log_id INT PRIMARY KEY
  , sys_conversation_id UNIQUEIDENTIFIER NOT NULL
  , CONSTRAINT fk_conversation_sys_reference_conversation_log_id 
      FOREIGN KEY (conversation_log_id) REFERENCES conversation_log (id));

CREATE TABLE message_log (
    conversation_log_id INT NOT NULL
  , id INT IDENTITY(1,1)
  , message_type SYSNAME
  , is_incoming BIT
  , send_time DATETIME
  , receive_time DATETIME
  , CONSTRAINT pk_message_log_conversation_log_id_id
      PRIMARY KEY (conversation_log_id, id));

CREATE TABLE request_log (
    conversation_log_id INT PRIMARY KEY
  , request_payload XML NOT NULL
  , response_payload XML
  , CONSTRAINT fk_request_log_conversation_log_id 
      FOREIGN KEY (conversation_log_id) REFERENCES conversation_log (id));

CREATE TABLE error_log (
    conversation_log_id INT PRIMARY KEY
  , error_time DATETIME
  , error_number INT
  , error_message NVARCHAR(MAX)
  , CONSTRAINT fk_error_log_conversation_log_id 
      FOREIGN KEY (conversation_log_id) REFERENCES conversation_log (id));
GO
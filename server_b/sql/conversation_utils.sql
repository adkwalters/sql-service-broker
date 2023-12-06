USE server_b_database
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON; -- required for XML methods
GO

-- Create an XML element from the input name and value
CREATE FUNCTION [dbo].[ufn_create_xml_element] (
      @name SYSNAME
    , @value SQL_VARIANT
)
RETURNS XML
AS
BEGIN
    RETURN (
        SELECT
              @name [@name]
            , sql_variant_property(@value, 'BaseType') [@base_type]
            , sql_variant_property(@value, 'Precision') [@precision]
            , sql_variant_property(@value, 'Scale') [@scale]
            , sql_variant_property(@value, 'MaxLength') [@max_length]
            , @value
        FOR XML PATH('Parameter'), TYPE);
END
GO
-- --============TEST============--
-- select dbo.ufn_create_xml_element('parameter_name', 'parameter_value');
-- go
-- --============TEST============--


-- Create an XML document to describe the input table
CREATE FUNCTION [dbo].[ufn_get_table_metadata_as_xml] (
      @table_name SYSNAME
)
RETURNS XML
AS
BEGIN
    DECLARE @table_query NVARCHAR(MAX) = N'SELECT TOP 1 * FROM ' + @table_name;

    RETURN (
        SELECT @table_name [Table/@name]
           ,(
                SELECT [column_ordinal] [@ordinal]
                    , [name] [@name]
                    , [system_type_name] [@type]
                FROM sys.dm_exec_describe_first_result_set(@table_query, NULL, NULL)
                WHERE [name] IS NOT NULL
                FOR XML PATH('Column'), TYPE
            )
        FOR XML PATH('TableMetadata'), TYPE);
END
GO
-- ============TEST============--
-- select dbo.ufn_get_table_metadata_as_xml('sys.dm_broker_queue_monitors');
-- go
-- ============TEST============--


-- Create an XML document from the input parameters to describe a procedure invocation
CREATE PROCEDURE [usp_output_sql_as_xml]
      @procedure_name SYSNAME
    , @return_xml BIT = 0
    , @param_1 SYSNAME = NULL, @value_1 SQL_VARIANT = NULL
    , @param_2 SYSNAME = NULL, @value_2 SQL_VARIANT = NULL
    , @param_3 SYSNAME = NULL, @value_3 SQL_VARIANT = NULL
    , @param_4 SYSNAME = NULL, @value_4 SQL_VARIANT = NULL
    , @param_5 SYSNAME = NULL, @value_5 SQL_VARIANT = NULL
    , @xml_output XML OUTPUT
AS
BEGIN TRY
    SET NOCOUNT ON;

    SET @xml_output = (
        SELECT 
              @procedure_name [Name]
            , @return_xml [ReturnXml]
            , (
                SELECT * FROM
                (
                    SELECT dbo.ufn_create_xml_element(@param_1, @value_1) [*] WHERE @value_1 IS NOT NULL
                    UNION ALL
                    SELECT dbo.ufn_create_xml_element(@param_2, @value_2) [*] WHERE @value_2 IS NOT NULL
                    UNION ALL
                    SELECT dbo.ufn_create_xml_element(@param_3, @value_3) [*] WHERE @value_3 IS NOT NULL
                    UNION ALL
                    SELECT dbo.ufn_create_xml_element(@param_4, @value_4) [*] WHERE @value_4 IS NOT NULL
                    UNION ALL
                    SELECT dbo.ufn_create_xml_element(@param_5, @value_5) [*] WHERE @value_5 IS NOT NULL
                ) [*]
                FOR XML PATH(''), TYPE
              ) [Parameters]
        FOR XML PATH('Procedure'), TYPE);
END TRY
BEGIN CATCH
    SELECT 'The following error occurred when executing usp_output_sql_as_xml: '
        + ERROR_MESSAGE() [error];
END CATCH
GO
-- --============TEST============--
-- declare @create_test_table xml;
-- exec usp_output_sql_as_xml
--      @procedure_name = N'usp_create_test_table'
--    , @xml_output = @create_test_table output;
-- select @create_test_table;

-- declare @insert_test_table xml;
-- exec usp_output_sql_as_xml
--      @procedure_name = N'usp_insert_test_table'
--    , @param_1 = N'@insert_string', @value_1 = 'Hello, World'
--    , @param_2 = N'@waitfor_delay', @value_2 = '00:01:00'
--    , @xml_output = @insert_test_table output;
-- select @insert_test_table;

-- declare @read_test_table xml;
-- exec usp_output_sql_as_xml
--      @procedure_name = N'usp_read_test_table'
--    , @return_xml = 1
--    , @xml_output = @read_test_table output
-- select @read_test_table;
-- go
-- --============TEST============--


-- Execute the procedure invocation described in the input XML document
CREATE PROCEDURE [usp_execute_sql_from_xml] (
      @xml_in XML
    , @xml_out XML = NULL OUTPUT)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @procedure_name SYSNAME
        , @return_xml BIT

        , @param_count INT
        , @param_name SYSNAME
        , @param_type SYSNAME
        , @param_precision INT
        , @param_scale INT
        , @param_length INT
        , @param_type_full NVARCHAR(300)

        , @exec_param_name SYSNAME
        , @exec_param_declaration NVARCHAR(MAX)
        , @exec_param_assignment NVARCHAR(MAX)
        , @exec_param_invocation NVARCHAR(MAX)

        , @exec_statement NVARCHAR(MAX)
        , @exec_parameters NVARCHAR(MAX);
        
    SELECT @procedure_name = @xml_in.value(N'(Procedure/Name)[1]', N'SYSNAME')
        , @return_xml = @xml_in.value(N'(Procedure/ReturnXml)[1]', N'BIT');

    -- Loop through each described parameter and create the dynamic strings
    -- required to create the full procedure execution statement
    
    SELECT @param_count = 0
        , @exec_param_declaration = N''
        , @exec_param_assignment = N''
        , @exec_param_invocation = N'';

    DECLARE param_cursor CURSOR FOR
        SELECT col.value(N'@name', N'SYSNAME')
            , col.value(N'@base_type', N'SYSNAME')
            , col.value(N'@precision', N'INT')
            , col.value(N'@scale', N'INT')
            , col.value(N'@max_length', N'INT')
        FROM @xml_in.nodes(N'Procedure/Parameters/Parameter') tab(col);

    OPEN param_cursor;
    FETCH NEXT FROM param_cursor INTO
          @param_name
        , @param_type
        , @param_precision
        , @param_scale
        , @param_length;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @param_count += 1;

        SET @param_type_full = @param_type + CASE
            WHEN @param_type IN (N'CHAR', N'NCHAR', N'VARCHAR', N'NVARCHAR', N'BINARY', N'VARBINARY')
                THEN CONCAT(N'(', @param_length, N')')
            WHEN @param_type IN (N'NUMERIC')
                THEN CONCAT(N'(', @param_precision, N',', @param_scale, N')')
            ELSE N'' END;

        IF (@param_name IS NULL
                OR @param_type IS NULL
                OR @param_type_full IS NULL
                OR CHARINDEX(N'''', @param_name) > 0
                OR CHARINDEX(N'''', @param_type_full) > 0)
            RAISERROR(N'An error occurred while building parameter %i: %s = %s %i:%i:%i', 16, 1,
                  @param_count, @param_name, @param_type
                , @param_precision, @param_scale, @param_length);

        SET @exec_param_name = CONCAT('@p_', @param_count);
            --> '@p_1'

        SET @exec_param_declaration += CONCAT(
              N'DECLARE '
            , @exec_param_name, N' ', @param_type_full, N'; ');
            --> 'declare @p_1 char(1);'

        SET @exec_param_assignment += CONCAT(
              N'SELECT '
            , @exec_param_name, N' = @xml_in.value(N''(Procedure/Parameters/Parameter)'
            , N'[', @param_count, N']''', N', N''', @param_type_full, '''); ');
            --> 'select @p_1 = @xml_in.value(N'(Procedure/Parameters/Parameter)[1]', N'(char(1))');'

        SET @exec_param_invocation += CONCAT(
              CASE WHEN @param_count > 1 THEN ', ' END
            , @param_name, N' = ', @exec_param_name);
            --> '@my_var = @p_1, @my_var2 = @p2'

        FETCH NEXT FROM param_cursor INTO @param_name
            , @param_type
            , @param_precision
            , @param_scale
            , @param_length;
    END
    CLOSE param_cursor;
    DEALLOCATE param_cursor;

    SET @exec_statement = CONCAT(
          @exec_param_declaration
        , @exec_param_assignment
        , N' EXEC ', QUOTENAME(@procedure_name)
        , CASE WHEN @param_count > 0 THEN CONCAT(N' ', @exec_param_invocation) END
        , CASE WHEN @param_count > 0 AND @return_xml = 1 THEN N', ' ELSE N' ' END
        , CASE WHEN @return_xml = 1 THEN N'@xml_output = @xml_out OUTPUT' END);

    SET @exec_parameters = CONCAT(
          N'@xml_in xml'
        , CASE WHEN @return_xml = 1 THEN N', @xml_out XML OUTPUT' END);

    -- Execute the full procedure statement with its parameters
    IF @return_xml = 0
        EXEC sp_executesql @exec_statement, @exec_parameters, @xml_in;
    ELSE
        EXEC sp_executesql @exec_statement, @exec_parameters, @xml_in, @xml_out OUTPUT;
END
GO
-- --============TEST============--
-- declare @usp_create_test_table xml = '<Procedure><Name>usp_create_test_table</Name><ReturnXml>0</ReturnXml></Procedure>';
-- exec usp_execute_sql_from_xml @xml_in = @usp_create_test_table;

-- declare @usp_insert_test_table xml = '<Procedure><Name>usp_insert_test_table</Name><ReturnXml>0</ReturnXml><Parameters><Parameter name="@insert_string" base_type="varchar" precision="0" scale="0" max_length="12">Hello, World</Parameter><Parameter name="@waitfor_delay" base_type="varchar" precision="0" scale="0" max_length="8">00:00:01</Parameter></Parameters></Procedure>';
-- exec usp_execute_sql_from_xml @xml_in = @usp_insert_test_table;

-- declare @usp_read_test_table xml = '<Procedure><Name>usp_read_test_table</Name><ReturnXml>1</ReturnXml></Procedure>';
-- declare @xml_output xml;
-- exec usp_execute_sql_from_xml @xml_in = @usp_read_test_table, @xml_out = @xml_output output;
-- select @xml_output;
-- go
-- --============TEST============--


-- Read the input XML document as a result set
CREATE PROCEDURE usp_output_xml_as_table (
    @xml_input XML
)
AS
BEGIN TRY
    SET NOCOUNT ON

    DECLARE @select_stmt NVARCHAR(MAX)
        , @col_name SYSNAME
        , @col_type SYSNAME
        , @table_name SYSNAME
        , @request_status NVARCHAR(50)

    DECLARE @table_columns TABLE (
          col_ordinal INT
        , col_name SYSNAME
        , col_type SYSNAME)

    SELECT @table_name = x.value('local-name(.)', 'SYSNAME') FROM @xml_input.nodes('/*') t(x);

    -- Handle documents without a result set
    IF @table_name IS NULL
    BEGIN
        SELECT 'The conversation is awaiting a response' [conversation_response]
        RETURN
    END
    ELSE IF @table_name = N'Status'
    BEGIN
        SELECT @request_status =  @xml_input.value('(Status)[1]', 'NVARCHAR(50)')
        SELECT CONCAT('The conversation ended with status ''',  @request_status, '''.') [conversation_response];
        RETURN
    END

    -- Get the schema of the result set
    INSERT INTO @table_columns (col_ordinal, col_name, col_type)
    SELECT x.value('@ordinal', 'INT') [col_ordinal]
        , x.value('@name', 'SYSNAME') [col_name]
        , x.value('@type', 'NVARCHAR(256)') [col_type]
    FROM @xml_input.nodes('Result/TableMetadata/Column') t(x);

    -- Loop through each described result set column and create a dynamic string
    -- required to create the full select statement

    SET @select_stmt = N'SELECT ';

    DECLARE col_cursor CURSOR FOR
        SELECT col_name, col_type
        FROM @table_columns
        ORDER BY col_ordinal;

    OPEN col_cursor;
    FETCH NEXT FROM col_cursor INTO @col_name, @col_type;

    WHILE @@FETCH_STATUS = 0
    BEGIN

        SET @select_stmt += N'x.value(''(' + @col_name + ')[1]'', ''' + @col_type + ''') [' + @col_name + '], ';

        FETCH NEXT FROM col_cursor INTO @col_name, @col_type;
    END
    CLOSE col_cursor;
    DEALLOCATE col_cursor;

    SET @select_stmt = LEFT(@select_stmt, LEN(@select_stmt) -1);
    SET @select_stmt += N' FROM @xml.nodes(''Result/TableData/Row'') tbl(x)';

    -- Execute the full select statement
    EXEC sp_executesql @select_stmt, N'@xml XML', @xml_input;

END TRY
BEGIN CATCH
    SELECT 'The input XML document is not readable by this procedure.' [error];
END CATCH
GO
-- --============TEST============--
-- declare @result_set_xml xml = '<Result><TableMetadata><Table name="test_table" /><Column ordinal="1" name="id" type="int" /><Column ordinal="2" name="string_value" type="nvarchar(max)" /></TableMetadata><TableData><Row><id>1</id><string_value>Hello, World</string_value></Row><Row><id>2</id><string_value>Hello again, World</string_value></Row></TableData></Result>';
-- exec usp_output_xml_as_table @result_set_xml;
-- --============TEST============--


-- Read the response XML document for the input conversation as a result set
CREATE PROCEDURE usp_show_conversation_response (
    @conversation_log_id INT
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @response_xml XML;

    SELECT @conversation_log_id = id
    FROM conversation_log
    WHERE id = @conversation_log_id;

    IF @conversation_log_id IS NULL
    BEGIN
        SELECT 'No conversation exists with the given conversation_log_id.' [error];
        RETURN
    END

    SELECT @response_xml = response_payload
    FROM request_log
    WhERE conversation_log_id = @conversation_log_id;

    EXEC usp_output_xml_as_table @xml_input = @response_xml;
END
GO
-- --============TEST============--
-- exec usp_show_conversation_response @conversation_log_id = 1;
-- --============TEST============--
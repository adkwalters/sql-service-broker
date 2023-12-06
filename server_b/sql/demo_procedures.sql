USE server_b_database
GO

-- These example procedures demonstrate queries that can be executed indirectly by the far service
-- They include DDL, DQL, and DML. TCL is handled by the application itself

SET ANSI_NULLS ON;
GO

CREATE PROCEDURE usp_demo_drop_table
AS
BEGIN
    IF OBJECT_ID(N'dbo.demo_table', N'U') IS NOT NULL
        DROP TABLE demo_table;
END
GO


CREATE PROCEDURE usp_demo_create_table
AS
BEGIN
    CREATE TABLE demo_table (
          id INT IDENTITY(1,1) PRIMARY KEY
        , inserted_value NVARCHAR(MAX));
END
GO


CREATE PROCEDURE usp_demo_read_table (
    @xml_output XML OUTPUT
)
AS
BEGIN
    SET @xml_output = (
        SELECT
        (
            SELECT dbo.ufn_get_table_metadata_as_xml('demo_table')
        )
       ,(
            SELECT id
                , inserted_value
            FROM demo_table
            FOR XML PATH('Row'), ROOT('TableData'), TYPE
        )
        FOR XML PATH('Result'), TYPE
    );
END
GO


CREATE PROCEDURE usp_demo_insert_table (
      @insert_string NVARCHAR(MAX)
    , @waitfor_delay NVARCHAR(10) = '00:00:00'
)
AS
BEGIN
    INSERT INTO demo_table (inserted_value)
    VALUES (@insert_string);

    WAITFOR DELAY @waitfor_delay;
END
GO


CREATE PROC usp_simulate_error
AS
BEGIN
    DECLARE @table_with_pk TABLE (id INT PRIMARY KEY);

    INSERT INTO @table_with_pk (id) 
    VALUES (1), (1);
END
GO
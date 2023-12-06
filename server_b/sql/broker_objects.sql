USE server_b_database
GO

-- Create XML schemas to control the structure of request and response messages
CREATE XML SCHEMA COLLECTION request_message_schema AS
N'<?xml version="1.0" encoding="utf-16"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="Message">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="MessageHeader">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="SendType" type="xs:string"/>
                            <xs:element name="SendTime" type="xs:dateTime"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="MessagePayload">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="Procedure">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="Name" type="xs:string"/>
                                        <xs:element name="ReturnXml" type="xs:boolean"/>
                                        <xs:element name="Parameters" minOccurs="0" maxOccurs="1">
                                            <xs:complexType>
                                                <xs:sequence>
                                                    <xs:element name="Parameter" minOccurs="0" maxOccurs="unbounded">
                                                        <xs:complexType>
                                                            <xs:simpleContent>
                                                                <xs:extension base="xs:string">
                                                                    <xs:attribute name="name" type="xs:string"/>
                                                                    <xs:attribute name="base_type" type="xs:string"/>
                                                                    <xs:attribute name="precision" type="xs:string"/>
                                                                    <xs:attribute name="scale" type="xs:string"/>
                                                                    <xs:attribute name="max_length" type="xs:string"/>
                                                                </xs:extension>
                                                            </xs:simpleContent>
                                                        </xs:complexType>
                                                    </xs:element>
                                                </xs:sequence>
                                            </xs:complexType>
                                        </xs:element>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>';

CREATE XML SCHEMA COLLECTION response_message_schema AS
N'<?xml version="1.0" encoding="utf-16"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="Message">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="MessageHeader">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="ReceiveType" type="xs:string"/>
                            <xs:element name="ReceiveTime" type="xs:dateTime"/>
                            <xs:element name="SendType" type="xs:string"/>
                            <xs:element name="SendTime" type="xs:dateTime"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="MessagePayload">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:any minOccurs="0" maxOccurs="unbounded" processContents="lax"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>';

-- Create message types and contracts
CREATE MESSAGE TYPE request_message
    VALIDATION = VALID_XML WITH SCHEMA COLLECTION request_message_schema;

CREATE MESSAGE TYPE response_message
    VALIDATION = VALID_XML WITH SCHEMA COLLECTION response_message_schema;

CREATE CONTRACT service_contract (
      request_message SENT BY INITIATOR
    , response_message SENT BY TARGET);

-- Create broker objects and set a route
CREATE QUEUE server_b_service_queue
    WITH 
        STATUS = ON
       , RETENTION = OFF
       , ACTIVATION (
            STATUS = ON
           , PROCEDURE_NAME = usp_activation_procedure
           , MAX_QUEUE_READERS = 5
           , EXECUTE AS OWNER)
       , POISON_MESSAGE_HANDLING (STATUS = ON);

CREATE SERVICE server_b_service
    ON QUEUE server_b_service_queue (
        service_contract);

CREATE ROUTE server_a_service_route
	WITH 
        SERVICE_NAME = 'server_a_service',
	    ADDRESS	= 'TCP://server_a:4022';
GO

-- Allow all users to initiate conversations
GRANT SEND ON SERVICE::[server_b_service] TO PUBLIC;
GO

-- Create the service broker endpoint 
-- and sign it using the certificate created in broker_certificate_creation.sql
CREATE ENDPOINT server_b_service_endpoint
    STATE = STARTED
    AS TCP (LISTENER_PORT = 4022)
    FOR SERVICE_BROKER (
        AUTHENTICATION = CERTIFICATE server_b_certificate);
GO
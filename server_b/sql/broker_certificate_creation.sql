USE master
GO

-- Set the master key for base encryption
CREATE MASTER KEY 
    ENCRYPTION BY PASSWORD = 'SecretPa$$word_Srvr_B';

-- Create a self-signed TLS certificate for the service broker endpoint
CREATE CERTIFICATE server_b_certificate
    WITH SUBJECT = 'For server_b Service Broker authentication',
    START_DATE = '2023-07-16',
    EXPIRY_DATE = '2034-07-16';

-- Save the certificate and its private key to the shared volume
BACKUP CERTIFICATE server_b_certificate
    TO FILE = '/tmp/server_b_certificate.crt'
    WITH PRIVATE KEY (
        FILE ='/tmp/server_b_certificate_private_key.pvk',
        ENCRYPTION BY PASSWORD = 'SecretPa$$word_Srvr_B_Crt');
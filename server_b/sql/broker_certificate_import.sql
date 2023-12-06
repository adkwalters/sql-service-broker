USE master
GO

-- Note, the possession of the certificate and private key
-- that is used to sign the endpoint of another service
-- is sufficient to authenticate a connection to that endpoint

-- Import the certificate for server_a Service Broker endpoint
CREATE CERTIFICATE server_a_certificate
    FROM FILE = '/tmp/certs/server_b/server_a_certificate.crt'
    WITH PRIVATE KEY (
        FILE = '/tmp/certs/server_b/server_a_certificate_private_key.pvk',
        DECRYPTION BY PASSWORD = 'SecretPa$$word_Srvr_A_Crt');
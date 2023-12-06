# Export the environment variables for SQL Server
export $(xargs < /tmp/sapassword.env)
export $(xargs < /tmp/sqlcmd.env)

# Restart SQL Server and await its initialisation
( /opt/mssql/bin/sqlservr & ) \
    | grep -q "Recovery is complete."

# Import the certificate for Server A's service broker endpoint
/opt/mssql-tools/bin/sqlcmd \
    -i /tmp/broker_certificate_import.sql

# Keep the container open indefinitely
sleep infinity
# Export the environment variables for SQL Server
export $(xargs < /tmp/sapassword.env)
export $(xargs < /tmp/sqlcmd.env)

# Copy the SQL Server configuration file into the container
cp /tmp/mssql.conf /var/opt/mssql/mssql.conf

# Start SQL Server and await its initialisation
( /opt/mssql/bin/sqlservr & ) \
    | grep -q "Recovery is complete."

# Create the objects required for the service broker: TODO: update
#   - a database, a service, a queue, a route, a contract with message types,
#     procedures to send and receive messages, and a table to store messages
#   - a self-signed public TLS certificate with a private key
#   - a service broker endpoint signed with the public certificate
/opt/mssql-tools/bin/sqlcmd \
    -i /tmp/logging_objects.sql \
    -i /tmp/conversation_utils.sql \
    -i /tmp/broker_certificate_creation.sql \
    -i /tmp/activation_procedure.sql \
    -i /tmp/broker_objects.sql \
    -i /tmp/conversation_inspection.sql \
    -i /tmp/conversation_management.sql \
    -i /tmp/demo_procedures.sql \
    -i /tmp/demo_scripts.sql
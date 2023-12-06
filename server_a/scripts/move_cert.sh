# Note, the following commands contain checks before execution
# to allow execution to be skipped upon restarting the container

# If it does not already exist,
#   create a folder on the shared volume for Server B
#   to store the certificate for Server A's service broker endpoint
[ -d "/tmp/certs/server_b" ] || mkdir /tmp/certs/server_b

# If the certificate exists,
#   move the certificate and its private key to Server B's folder
if [ -f "/tmp/server_a_certificate.crt" ]; then
    mv /tmp/server_a_certificate.crt /tmp/certs/server_b
    mv /tmp/server_a_certificate_private_key.pvk /tmp/certs/server_b
fi

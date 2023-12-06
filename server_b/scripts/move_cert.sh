# Note, the following commands contain checks before execution
# to allow execution to be skipped upon restarting the container

# If it does not already exist,
#   create a folder on the shared volume for Server A
#   to store the certificate for Server B's service broker endpoint
[ -d "/tmp/certs/server_a" ] || mkdir /tmp/certs/server_a

# If the certificate exists,
#   move the certificate and its private key to Server A's folder
if [ -f "/tmp/server_b_certificate.crt" ]; then
    mv /tmp/server_b_certificate.crt /tmp/certs/server_a
    mv /tmp/server_b_certificate_private_key.pvk /tmp/certs/server_a
fi
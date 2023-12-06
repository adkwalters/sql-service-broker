# Get the Docker image for SQL Server 2022
FROM mcr.microsoft.com/mssql/server:2022-latest

# Ensure priviledged user is set
USER root

# Create directories and set permissions for the default, non-priviledged, SQL Server user
WORKDIR /tmp
RUN mkdir /tmp/certs
RUN chown -R mssql /tmp

# Copy scripts into the container and mark as executable
COPY sql /tmp/
COPY scripts /tmp/
RUN chmod +x /tmp/*.sh

# Switch to the non-priviledged user
USER mssql

# Begin application initialisation
RUN /bin/bash /tmp/initialise_app.sh

# Complete application initialisation
CMD ["sh", "-c", "/tmp/move_cert.sh && /tmp/import_cert.sh"]
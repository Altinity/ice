# Testing SSL Certificate Verification Skip

This guide shows how to test the `--insecure` / `--ssl-no-verify` flag with self-signed certificates.

## Option 1: Quick Test with a Self-Signed HTTPS Server

### 1. Generate a self-signed certificate

```bash
# Generate a self-signed certificate for testing
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

### 2. Set up an HTTPS proxy to ice-rest-catalog

Create a simple nginx config to add SSL in front of the catalog:

**nginx-ssl.conf**:
```nginx
events {
    worker_connections 1024;
}

http {
    server {
        listen 5443 ssl;
        server_name localhost;

        ssl_certificate /etc/nginx/cert.pem;
        ssl_certificate_key /etc/nginx/key.pem;

        location / {
            proxy_pass http://ice-rest-catalog:5000;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
```

**docker-compose.ssl-test.yaml**:
```yaml
services:
  nginx-ssl:
    image: nginx:alpine
    ports:
      - "5443:5443"
    volumes:
      - ./cert.pem:/etc/nginx/cert.pem:ro
      - ./key.pem:/etc/nginx/key.pem:ro
      - ./nginx-ssl.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - ice-rest-catalog
```

### 3. Start the services

```bash
# Start the regular docker-compose setup
docker-compose up -d

# Start the SSL proxy
docker-compose -f docker-compose.ssl-test.yaml up -d
```

### 4. Test the ice CLI

First, format and build the code:
```bash
cd /home/kanthi/Documents/GITHUB/ALTINITY/ice
mvn com.spotify.fmt:fmt-maven-plugin:format -pl ice
mvn clean install -pl ice -am -DskipTests
```

Create a test config file `.ice-ssl-test.yaml`:
```yaml
uri: https://localhost:5443
bearerToken: foo
warehouse: s3://bucket1
sslVerify: false  # Config file option
s3:
  endpoint: http://localhost:8999
  pathStyleAccess: true
  accessKeyID: miniouser
  secretAccessKey: miniopassword
  region: minio
```

**Test 1: Without --insecure flag (should fail with SSL error)**:
```bash
./ice/target/ice-jar -c .ice-ssl-test.yaml check
# Expected: SSL certificate verification error
```

**Test 2: With --insecure flag (should succeed)**:
```bash
./ice/target/ice-jar -c .ice-ssl-test.yaml --insecure check
# Expected: OK
```

**Test 3: Using config file sslVerify option**:
With `sslVerify: false` in the config file:
```bash
./ice/target/ice-jar -c .ice-ssl-test.yaml check
# Expected: OK (with warning about disabled SSL verification)
```

**Test 4: Test other commands**:
```bash
# List namespaces
./ice/target/ice-jar -c .ice-ssl-test.yaml --insecure describe

# Create a namespace
./ice/target/ice-jar -c .ice-ssl-test.yaml --insecure create-namespace test_ns

# Create a table
./ice/target/ice-jar -c .ice-ssl-test.yaml --insecure create-table test_ns.test_table \
  --schema '[{"name":"id","type":"int"},{"name":"name","type":"string"}]'
```

## Option 2: Test with Public Self-Signed Certificate Server

You can also test against a public service with a self-signed certificate:

```bash
# This will fail due to self-signed certificate
./ice/target/ice-jar -c config.yaml check

# This will work with --insecure
./ice/target/ice-jar -c config.yaml --insecure check
```

## Option 3: Test with Internal CA Certificate

If you have an internal CA, you can test the custom CA bundle feature:

```yaml
uri: https://internal-catalog.company.local:5443
caCrt: |
  -----BEGIN CERTIFICATE-----
  MIIDXTCCAkWgAwIBAgIJAKJ...
  -----END CERTIFICATE-----
bearerToken: foo
```

This should work without `--insecure` because the CA certificate is provided.

## Expected Behavior

### Without SSL verification skip:
- ❌ Fails with certificate verification errors for self-signed certificates
- ✅ Works with valid certificates from trusted CAs
- ✅ Works when custom CA certificate is provided via `caCrt`

### With SSL verification skip (`--insecure` or `sslVerify: false`):
- ⚠️ Displays warning: "SSL certificate verification is DISABLED. This is insecure and should only be used for development."
- ✅ Works with any certificate (self-signed, expired, wrong hostname, etc.)
- ⚠️ **INSECURE**: Should only be used for development/testing

## Command-line Flags

Two equivalent options:
- `--insecure` - Shorter, commonly used in tools like curl
- `--ssl-no-verify` - More explicit about what it does

Both flags:
- Override the config file `sslVerify` setting
- Apply to all subcommands (check, describe, scan, insert, etc.)
- Display a warning message when SSL verification is disabled

## Config File Option

```yaml
sslVerify: false  # Skip SSL certificate verification
```

Note: The command-line flag takes precedence over the config file option.







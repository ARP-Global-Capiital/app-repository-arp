# SFTP Usage Guide

## Quick Reference

### Connection Details
```
Host: 51.112.188.123
Port: 2222
Username: broadridge
Password: MZ7pCuUrrKKJf3
```

## Connecting via Command Line

### Linux/Mac:
```bash
sftp -P 2222 broadridge@51.112.188.123
```

### Windows (PowerShell):
```powershell
sftp -P 2222 broadridge@51.112.188.123
```

## Common SFTP Commands

Once connected, you'll see the `sftp>` prompt.

### Navigation & Listing

```bash
# Show current remote directory
pwd

# List remote files
ls
ls -la              # Detailed listing

# Show local directory
lpwd

# List local files
lls
lls -la             # Detailed listing

# Change remote directory
cd directory_name

# Change local directory
lcd /path/to/local/directory
```

### Uploading Files

```bash
# Upload a single file
put /path/to/local/file.txt

# Upload file with different name
put /path/to/local/file.txt remote-name.txt

# Upload multiple files
mput *.txt

# Upload a directory recursively
put -r /path/to/local/directory
```

### Downloading Files

```bash
# Download a single file
get remote-file.txt

# Download to specific location
get remote-file.txt /path/to/local/file.txt

# Download multiple files
mget *.txt

# Download a directory recursively
get -r remote-directory
```

### File Management

```bash
# Create remote directory
mkdir directory_name

# Remove remote file
rm file.txt

# Remove remote directory
rmdir directory_name

# Rename remote file
rename old-name.txt new-name.txt

# Check file permissions
ls -la
```

### Other Useful Commands

```bash
# Get help
help

# Show SFTP version
version

# Exit SFTP
bye
# or
exit
# or
quit
```

## Example Upload Session

```bash
$ sftp -P 2222 broadridge@51.112.188.123
broadridge@51.112.188.123's password: [enter password]
Connected to 51.112.188.123.

sftp> pwd
Remote working directory: /home/broadridge/upload

sftp> lcd ~/Documents
sftp> lls
file1.txt  file2.csv  report.pdf

sftp> put file1.txt
Uploading file1.txt to /home/broadridge/upload/file1.txt
file1.txt                                     100%   1024     15.2KB/s   00:00

sftp> put file2.csv
Uploading file2.csv to /home/broadridge/upload/file2.csv
file2.csv                                     100%   5120     45.1KB/s   00:00

sftp> ls
file1.txt  file2.csv

sftp> bye
```

## Using GUI Clients

### WinSCP (Windows)
1. Download from: https://winscp.net/
2. New Session:
   - File protocol: SFTP
   - Host name: 51.112.188.123
   - Port number: 2222
   - User name: broadridge
   - Password: MZ7pCuUrrKKJf3
3. Click Login

### FileZilla (Windows/Mac/Linux)
1. Download from: https://filezilla-project.org/
2. File → Site Manager → New Site
   - Protocol: SFTP
   - Host: 51.112.188.123
   - Port: 2222
   - Logon Type: Normal
   - User: broadridge
   - Password: MZ7pCuUrrKKJf3
3. Click Connect

### Cyberduck (Mac)
1. Download from: https://cyberduck.io/
2. Open Connection:
   - SFTP (SSH File Transfer Protocol)
   - Server: 51.112.188.123
   - Port: 2222
   - Username: broadridge
   - Password: MZ7pCuUrrKKJf3
3. Connect

## Batch Upload Script Example

### Linux/Mac:
```bash
#!/bin/bash
# batch-upload.sh

HOST="51.112.188.123"
PORT="2222"
USER="broadridge"
PASS="MZ7pCuUrrKKJf3"
FILES="/path/to/local/files/*.csv"

sshpass -p "$PASS" sftp -P $PORT $USER@$HOST << EOF
cd upload
mput $FILES
ls -la
bye
EOF
```

### Windows PowerShell:
```powershell
# batch-upload.ps1

$host = "51.112.188.123"
$port = "2222"
$user = "broadridge"
$files = "C:\path\to\files\*.csv"

# Create batch file for SFTP
@"
cd upload
mput $files
ls -la
bye
"@ | sftp -P $port $user@$host
```

## Troubleshooting

### Connection Refused
- Check if your IP is whitelisted in `hosts.allow`
- Verify firewall/security group allows your IP
- Confirm port 2222 is correct

### Authentication Failed
- Verify password: `MZ7pCuUrrKKJf3`
- Confirm username: `broadridge`
- Check for typos

### Permission Denied
- You can only upload to: `/home/broadridge/upload`
- Files are stored on server at: `/code/ARP_Global/data/`

### Connection Timeout
- Check network connectivity
- Verify server is running: `docker compose ps`
- Check logs: `docker compose logs sftp`

## Server-Side File Access

Files uploaded via SFTP are stored at:
```
/code/ARP_Global/data/
```

To view uploaded files on the server:
```bash
# List files
ls -la /code/ARP_Global/data/

# View file content
cat /code/ARP_Global/data/filename.txt

# Copy file
cp /code/ARP_Global/data/filename.txt /destination/

# Process files
for file in /code/ARP_Global/data/*.csv; do
    echo "Processing $file"
    # Your processing logic here
done
```

## Security Notes

1. Only whitelisted IPs can connect (Broadridge + testing IPs)
2. Password authentication is required
3. Files are stored in isolated Docker volume
4. Regular monitoring via logs is recommended
5. Change password periodically for security

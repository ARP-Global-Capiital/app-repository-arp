# ARP Global SFTP Server

Docker-based SFTP server for secure file transfers with Broadridge.

## Quick Start

1. **Update Configuration**
   ```bash
   # Edit .env file with your credentials and hostname
   nano .env
   ```

2. **Start SFTP Server**
   ```bash
   docker compose up -d
   ```

3. **Stop SFTP Server**
   ```bash
   docker compose down
   ```

## Configuration

### Update .env File

Before starting the server, update the following in `.env`:

- `SFTP_USER`: Username for SFTP access (default: broadridge)
- `SFTP_PASSWORD`: Strong password for authentication
- `SFTP_PORT`: Port for SFTP service (default: 2222)
- `SFTP_HOST`: Your server's hostname or IP address

### Firewall Configuration

**IMPORTANT**: This server runs on AWS EC2. Use AWS Security Groups for IP whitelisting.

📋 **See [AWS_SECURITY_GROUP_SETUP.md](AWS_SECURITY_GROUP_SETUP.md) for detailed setup instructions**

**Quick Summary:**
1. Go to AWS Console → EC2 → Security Groups
2. Find security group for instance `i-0ff7b3238c078dffc`
3. Add inbound rules for port 2222 with Broadridge IPs

**Broadridge IPs to whitelist:**

**Individual IPs (recommended):**
- 38.101.185.196/32
- 38.103.44.196/32
- 74.119.32.196/32
- 74.119.33.196/32
- 74.119.34.196/32
- 74.119.36.196/32

**Or use Network Ranges (/28 CIDR blocks):**
- 38.101.185.192/28 and 38.103.44.192/28 (covers all 32 entries for prod and DR)
- 74.119.32.192/28, 74.119.33.192/28, 74.119.34.192/28, 74.119.36.192/28

**Testing IP (remove after testing):**
- 92.97.177.239/32

## Connection Details (Share with Broadridge)

Provide the following information to Broadridge:

```
Protocol: SFTP
Hostname: 51.112.188.123
Port: 2222
Username: broadridge
Password: MZ7pCuUrrKKJf3
Authentication: Password (no RSA key required)
```

## File Management

### Uploaded Files Location

Files uploaded by Broadridge will be stored in:
```
./data/
```

Or absolute path:
```
/code/ARP_Global/data/
```

### Accessing Uploaded Files

```bash
# List uploaded files
ls -la ./data/

# View file contents
cat ./data/filename.txt
```

### Backup Files

```bash
# Create backup
tar -czf sftp-backup-$(date +%Y%m%d).tar.gz data/
```

## Testing Connection

### From Linux/Mac:
```bash
sftp -P 2222 broadridge@51.112.188.123
```

### From Windows (using WinSCP or FileZilla):
- Protocol: SFTP
- Host: 51.112.188.123
- Port: 2222
- Username: broadridge
- Password: MZ7pCuUrrKKJf3

## Monitoring

### View Logs
```bash
docker compose logs -f sftp
```

### Check Container Status
```bash
docker compose ps
```

### Monitor File Uploads
```bash
watch -n 5 'ls -lth ./data/ | head -20'
```

## Security Best Practices

1. **Strong Password**: Use a complex password with uppercase, lowercase, numbers, and special characters
2. **Firewall Rules**: Only allow connections from whitelisted Broadridge IPs
3. **Regular Updates**: Keep the Docker image updated
   ```bash
   docker compose pull
   docker compose up -d
   ```
4. **Monitor Access**: Regularly check logs for unauthorized access attempts
5. **Backup Data**: Regularly backup the `data` directory

## Troubleshooting

### Cannot Connect to SFTP Server

1. **Check if container is running:**
   ```bash
   docker compose ps
   ```

2. **Check firewall rules:**
   ```bash
   # For iptables
   iptables -L -n | grep 2222

   # For ufw
   ufw status
   ```

3. **Verify port is open:**
   ```bash
   netstat -tulpn | grep :2222
   ```

4. **Check logs for errors:**
   ```bash
   docker compose logs sftp
   ```

### Permission Denied Errors

```bash
# Fix permissions on data directory
chmod 755 data/
```

### Reset Password

1. Edit `.env` file with new password
2. Restart container:
   ```bash
   docker compose down
   docker compose up -d
   ```

## Maintenance

### Update SFTP Server Image
```bash
docker compose pull
docker compose up -d
```

### Clear Old Files
```bash
# Delete files older than 90 days
find ./data/ -type f -mtime +90 -delete
```

## Support

For issues or questions, contact your system administrator.

## Architecture

- **Docker Image**: `atmoz/sftp` (lightweight, secure SFTP server)
- **Container Name**: `arp_global_sftp`
- **Network**: Isolated bridge network
- **Volumes**: Persistent storage for uploaded files
- **Restart Policy**: Unless stopped manually

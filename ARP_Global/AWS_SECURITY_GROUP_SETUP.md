# AWS Security Group Setup for SFTP Server

## Instance Information
- **Instance ID**: i-0ff7b3238c078dffc
- **Region**: me-central-1 (Middle East - UAE)
- **Public IP**: 51.112.188.123
- **SFTP Port**: 2222

## Step-by-Step Guide

### 1. Access AWS Console

1. Go to https://console.aws.amazon.com/
2. Sign in to your AWS account
3. Select region: **me-central-1** (Middle East - UAE)

### 2. Navigate to Security Groups

1. Go to **EC2** service
2. In the left sidebar, click **Security Groups** (under "Network & Security")
3. Find the security group attached to instance `i-0ff7b3238c078dffc`

### 3. Add Inbound Rules for SFTP

Click **Edit inbound rules** and add the following rules:

#### For Broadridge Production IPs:

| Type | Protocol | Port | Source | Description |
|------|----------|------|--------|-------------|
| Custom TCP | TCP | 2222 | 38.101.185.196/32 | Broadridge IP 1 |
| Custom TCP | TCP | 2222 | 38.103.44.196/32 | Broadridge IP 2 |
| Custom TCP | TCP | 2222 | 74.119.32.196/32 | Broadridge IP 3 |
| Custom TCP | TCP | 2222 | 74.119.33.196/32 | Broadridge IP 4 |
| Custom TCP | TCP | 2222 | 74.119.34.196/32 | Broadridge IP 5 |
| Custom TCP | TCP | 2222 | 74.119.36.196/32 | Broadridge IP 6 |

#### Optional - For Testing:

| Type | Protocol | Port | Source | Description |
|------|----------|------|--------|-------------|
| Custom TCP | TCP | 2222 | 92.97.177.239/32 | Testing IP (remove after testing) |

#### Using Network Ranges (Alternative):

If you prefer to use CIDR blocks instead of individual IPs:

| Type | Protocol | Port | Source | Description |
|------|----------|------|--------|-------------|
| Custom TCP | TCP | 2222 | 38.101.185.192/28 | Broadridge Prod Range 1 |
| Custom TCP | TCP | 2222 | 38.103.44.192/28 | Broadridge Prod Range 2 |
| Custom TCP | TCP | 2222 | 74.119.32.192/28 | Broadridge DR Range 1 |
| Custom TCP | TCP | 2222 | 74.119.33.192/28 | Broadridge DR Range 2 |
| Custom TCP | TCP | 2222 | 74.119.34.192/28 | Broadridge DR Range 3 |
| Custom TCP | TCP | 2222 | 74.119.36.192/28 | Broadridge DR Range 4 |

**Note**: Using /28 CIDR blocks covers all 32 entries for prod and DR as mentioned by Broadridge.

### 4. Save Rules

1. Click **Save rules**
2. The changes take effect immediately

### 5. Verify Rules

After adding the rules, verify that:

1. Broadridge IPs can connect:
   ```bash
   # From Broadridge network
   sftp -P 2222 broadridge@51.112.188.123
   ```

2. Other IPs are blocked:
   ```bash
   # From unauthorized IP
   sftp -P 2222 broadridge@51.112.188.123
   # Should timeout or be refused
   ```

## AWS CLI Commands (Alternative Method)

If you prefer to use AWS CLI, install it first:

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

Then configure and run:

```bash
# Configure AWS CLI
aws configure

# Get your security group ID
INSTANCE_ID="i-0ff7b3238c078dffc"
SG_ID=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --region me-central-1 --query 'Reservations[0].Instances[0].SecurityGroups[0].GroupId' --output text)

echo "Security Group ID: $SG_ID"

# Add Broadridge IPs
aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=38.101.185.196/32,Description="Broadridge IP 1"}]'

aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=38.103.44.196/32,Description="Broadridge IP 2"}]'

aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=74.119.32.196/32,Description="Broadridge IP 3"}]'

aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=74.119.33.196/32,Description="Broadridge IP 4"}]'

aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=74.119.34.196/32,Description="Broadridge IP 5"}]'

aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=74.119.36.196/32,Description="Broadridge IP 6"}]'

# Optional: Add testing IP
aws ec2 authorize-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=92.97.177.239/32,Description="Testing IP"}]'

# Verify rules
aws ec2 describe-security-groups --region me-central-1 --group-ids $SG_ID --query 'SecurityGroups[0].IpPermissions[?ToPort==`2222`]'
```

## Benefits of AWS Security Groups

1. **Network-level filtering**: Blocks traffic BEFORE it reaches your server
2. **No impact on other services**: Only affects port 2222
3. **Immediate effect**: Changes apply instantly
4. **Easy management**: Add/remove IPs via AWS Console
5. **Logging**: Can enable VPC Flow Logs to monitor traffic
6. **Stateful**: Return traffic is automatically allowed

## Remove Testing IP After Testing

Once testing is complete, remove your testing IP:

**Via AWS Console:**
1. Go to Security Groups
2. Find the rule with source `92.97.177.239/32`
3. Delete the rule

**Via AWS CLI:**
```bash
aws ec2 revoke-security-group-ingress --region me-central-1 --group-id $SG_ID --ip-permissions IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges='[{CidrIp=92.97.177.239/32}]'
```

## Monitoring

Monitor SFTP access attempts:

```bash
# View successful connections
cd /code/ARP_Global && docker compose logs sftp | grep "Accepted password"

# View all connection attempts
cd /code/ARP_Global && docker compose logs sftp | grep "from"

# Real-time monitoring
cd /code/ARP_Global && docker compose logs -f sftp
```

## Troubleshooting

**Cannot connect after adding rules:**
1. Verify the security group is attached to the correct instance
2. Check that the rules are for port 2222 (not 22)
3. Ensure the source IP is correct (use /32 for single IPs)
4. Verify the SFTP container is running: `docker compose ps`

**Need to add more IPs:**
Simply add new rules following the same format in the Security Group settings.

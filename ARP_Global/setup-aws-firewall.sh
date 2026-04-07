#!/bin/bash
# AWS Security Group Setup Script for SFTP Server
# This script adds firewall rules to allow only Broadridge IPs to access port 2222

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INSTANCE_ID="i-0ff7b3238c078dffc"
REGION="me-central-1"
SFTP_PORT=2222

# Broadridge IPs
BROADRIDGE_IPS=(
    "38.101.185.196/32:Broadridge IP 1"
    "38.103.44.196/32:Broadridge IP 2"
    "74.119.32.196/32:Broadridge IP 3"
    "74.119.33.196/32:Broadridge IP 4"
    "74.119.34.196/32:Broadridge IP 5"
    "74.119.36.196/32:Broadridge IP 6"
)

# Testing IP (optional)
TEST_IP="92.97.177.239/32"

echo -e "${GREEN}==================================================${NC}"
echo -e "${GREEN}  AWS Security Group Setup for SFTP Server${NC}"
echo -e "${GREEN}==================================================${NC}"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI is not installed${NC}"
    echo ""
    echo "To install AWS CLI:"
    echo "  curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'"
    echo "  unzip awscliv2.zip"
    echo "  sudo ./aws/install"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ AWS CLI is installed${NC}"

# Check if AWS credentials are configured
if ! aws sts get-caller-identity --region $REGION &>/dev/null; then
    echo -e "${RED}✗ AWS credentials are not configured${NC}"
    echo ""
    echo "To configure AWS CLI, run:"
    echo "  aws configure"
    echo ""
    echo "You'll need:"
    echo "  - AWS Access Key ID"
    echo "  - AWS Secret Access Key"
    echo "  - Default region: $REGION"
    echo "  - Default output format: json"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ AWS credentials are configured${NC}"
echo ""

# Get Security Group ID
echo "Getting Security Group ID for instance $INSTANCE_ID..."
SG_ID=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --region $REGION \
    --query 'Reservations[0].Instances[0].SecurityGroups[0].GroupId' \
    --output text 2>/dev/null)

if [ -z "$SG_ID" ] || [ "$SG_ID" == "None" ]; then
    echo -e "${RED}✗ Could not find security group for instance $INSTANCE_ID${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Found Security Group: $SG_ID${NC}"
echo ""

# Show current rules for port 2222
echo "Current rules for port $SFTP_PORT:"
aws ec2 describe-security-groups \
    --region $REGION \
    --group-ids $SG_ID \
    --query "SecurityGroups[0].IpPermissions[?ToPort==\`$SFTP_PORT\`]" \
    --output table 2>/dev/null || echo "No rules found"
echo ""

# Ask for confirmation
echo -e "${YELLOW}This will add the following rules to Security Group $SG_ID:${NC}"
echo "  - Allow Broadridge IPs (6 rules)"
echo "  - Optional: Allow testing IP $TEST_IP"
echo ""
read -p "Do you want to proceed? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Adding firewall rules..."
echo ""

# Add Broadridge IPs
for entry in "${BROADRIDGE_IPS[@]}"; do
    IFS=':' read -r ip description <<< "$entry"

    echo -n "Adding $description ($ip)... "

    if aws ec2 authorize-security-group-ingress \
        --region $REGION \
        --group-id $SG_ID \
        --ip-permissions IpProtocol=tcp,FromPort=$SFTP_PORT,ToPort=$SFTP_PORT,IpRanges="[{CidrIp=$ip,Description=\"$description\"}]" \
        &>/dev/null; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${YELLOW}Already exists or error${NC}"
    fi
done

# Ask about testing IP
echo ""
read -p "Do you want to add testing IP $TEST_IP? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -n "Adding testing IP ($TEST_IP)... "

    if aws ec2 authorize-security-group-ingress \
        --region $REGION \
        --group-id $SG_ID \
        --ip-permissions IpProtocol=tcp,FromPort=$SFTP_PORT,ToPort=$SFTP_PORT,IpRanges="[{CidrIp=$TEST_IP,Description=\"Testing IP - Remove after testing\"}]" \
        &>/dev/null; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${YELLOW}Already exists or error${NC}"
    fi
fi

echo ""
echo -e "${GREEN}==================================================${NC}"
echo -e "${GREEN}✓ Firewall rules added successfully!${NC}"
echo -e "${GREEN}==================================================${NC}"
echo ""

# Show final rules
echo "Final rules for port $SFTP_PORT:"
aws ec2 describe-security-groups \
    --region $REGION \
    --group-ids $SG_ID \
    --query "SecurityGroups[0].IpPermissions[?ToPort==\`$SFTP_PORT\`]" \
    --output table

echo ""
echo "You can now test the SFTP connection:"
echo "  sftp -P $SFTP_PORT broadridge@51.112.188.123"
echo ""
echo "To remove the testing IP later, run:"
echo "  aws ec2 revoke-security-group-ingress --region $REGION --group-id $SG_ID \\"
echo "    --ip-permissions IpProtocol=tcp,FromPort=$SFTP_PORT,ToPort=$SFTP_PORT,IpRanges='[{CidrIp=$TEST_IP}]'"
echo ""

#!/bin/bash
# SFTP Firewall Management Script
# This script manages iptables rules for the SFTP server on port 2222

SFTP_PORT=2222

# Broadridge IPs
BROADRIDGE_IPS=(
    "38.101.185.196"
    "38.103.44.196"
    "74.119.32.196"
    "74.119.33.196"
    "74.119.34.196"
    "74.119.36.196"
)

# Testing IP (optional - remove in production)
TEST_IP="92.97.177.239"

function enable_firewall() {
    echo "Enabling SFTP firewall rules..."

    # Remove any existing rules for port 2222
    clean_rules

    # Add ACCEPT rules for Broadridge IPs at the TOP of INPUT chain
    for ip in "${BROADRIDGE_IPS[@]}"; do
        sudo iptables -I INPUT 1 -p tcp --dport $SFTP_PORT -s $ip -j ACCEPT
        echo "✓ Allowed $ip"
    done

    # Optionally allow testing IP (comment out in production)
    # sudo iptables -I INPUT 1 -p tcp --dport $SFTP_PORT -s $TEST_IP -j ACCEPT
    # echo "✓ Allowed testing IP $TEST_IP"

    # Add DROP rule for all other IPs
    sudo iptables -I INPUT 1 -p tcp --dport $SFTP_PORT -j DROP
    echo "✓ Blocked all other IPs on port $SFTP_PORT"

    echo ""
    echo "Firewall enabled. Only Broadridge IPs can connect to SFTP."
    list_rules
}

function disable_firewall() {
    echo "Disabling SFTP firewall rules..."
    clean_rules
    echo "✓ All SFTP firewall rules removed"
    echo "⚠️  WARNING: SFTP server is now accessible from ANY IP!"
}

function clean_rules() {
    # Remove all rules related to port 2222 from INPUT chain
    while sudo iptables -D INPUT -p tcp --dport $SFTP_PORT -j DROP 2>/dev/null; do :; done
    for ip in "${BROADRIDGE_IPS[@]}"; do
        while sudo iptables -D INPUT -p tcp --dport $SFTP_PORT -s $ip -j ACCEPT 2>/dev/null; do :; done
    done
    while sudo iptables -D INPUT -p tcp --dport $SFTP_PORT -s $TEST_IP -j ACCEPT 2>/dev/null; do :; done
}

function list_rules() {
    echo ""
    echo "Current iptables rules for port $SFTP_PORT:"
    sudo iptables -L INPUT -n -v --line-numbers | grep -E "num|$SFTP_PORT" || echo "No rules found"
}

function add_ip() {
    if [ -z "$1" ]; then
        echo "Usage: $0 add-ip <IP_ADDRESS>"
        exit 1
    fi

    local ip=$1
    sudo iptables -I INPUT 1 -p tcp --dport $SFTP_PORT -s $ip -j ACCEPT
    echo "✓ Added $ip to allowed IPs"
    list_rules
}

function remove_ip() {
    if [ -z "$1" ]; then
        echo "Usage: $0 remove-ip <IP_ADDRESS>"
        exit 1
    fi

    local ip=$1
    sudo iptables -D INPUT -p tcp --dport $SFTP_PORT -s $ip -j ACCEPT 2>/dev/null
    echo "✓ Removed $ip from allowed IPs"
    list_rules
}

function show_help() {
    echo "SFTP Firewall Management Script"
    echo ""
    echo "Usage: $0 {enable|disable|list|add-ip|remove-ip|help}"
    echo ""
    echo "Commands:"
    echo "  enable      Enable firewall (allow only Broadridge IPs)"
    echo "  disable     Disable firewall (allow all IPs - NOT RECOMMENDED)"
    echo "  list        Show current firewall rules for SFTP"
    echo "  add-ip IP   Add an IP address to the whitelist"
    echo "  remove-ip IP Remove an IP address from the whitelist"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 enable"
    echo "  $0 add-ip 92.97.177.239"
    echo "  $0 list"
    echo "  $0 remove-ip 92.97.177.239"
    echo "  $0 disable"
}

# Main script logic
case "$1" in
    enable)
        enable_firewall
        ;;
    disable)
        disable_firewall
        ;;
    list)
        list_rules
        ;;
    add-ip)
        add_ip "$2"
        ;;
    remove-ip)
        remove_ip "$2"
        ;;
    help|--help|-h|"")
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac

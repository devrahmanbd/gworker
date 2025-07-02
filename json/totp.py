# totp.py

import pyotp
import re
import sys
from datetime import datetime


def parse_totp_secret(totp_field):
    """
    Converts a TOTP field like '2xeo iobs 2nmi qrje 7reh tsd7 s3yu kzly'
    into a base32 string suitable for pyotp.
    """
    # Remove spaces and ensure uppercase for base32 compatibility
    secret = re.sub(r"\s+", "", totp_field).upper()
    return secret


def read_gmail_accounts(filename="gmail_accounts.txt"):
    """
    Reads accounts from the file and returns a list of dicts:
    [{'email': ..., 'password': ..., 'totp_secret': ...}, ...]
    """
    accounts = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Split only on the first two colons
            parts = line.split(":", 2)
            if len(parts) == 3:
                email, password, totp_field = parts
                totp_secret = parse_totp_secret(totp_field)
                accounts.append(
                    {"email": email, "password": password, "totp_secret": totp_secret}
                )
            elif len(parts) == 2:
                email, password = parts
                accounts.append(
                    {"email": email, "password": password, "totp_secret": None}
                )
    return accounts


def get_current_totp(secret):
    """
    Returns the current TOTP code for a base32 secret.
    """
    try:
        totp = pyotp.TOTP(secret)
        return totp.now()
    except Exception as e:
        return f"Error: {e}"


def print_totp_codes(accounts):
    """
    Prints the current TOTP code for each account with a TOTP secret.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"TOTP codes at {now}:\n")
    for acc in accounts:
        if acc["totp_secret"]:
            code = get_current_totp(acc["totp_secret"])
            print(f"{acc['email']}: {code}")
        else:
            print(f"{acc['email']}: (no TOTP secret)")


if __name__ == "__main__":
    # Standalone script usage
    filename = sys.argv[1] if len(sys.argv) > 1 else "gmail_accounts.txt"
    accounts = read_gmail_accounts(filename)
    print_totp_codes(accounts)

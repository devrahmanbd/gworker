import os, base64, time, json, random, string, threading, configparser, glob, asyncio, concurrent.futures, re, argparse, sys, subprocess, tempfile, hashlib, httplib2, shutil
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formataddr
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from queue import Queue
from google_auth_httplib2 import AuthorizedHttp
from gsend import MemoryOptimizedRecipientLoader
from gsend import RateLimitManager
from gsend import MemoryManager
from gsend import OptimizedEmailWorkerQueue
from gsend import EnhancedGmailBulkSender
from gsend import CLIInterface

try:
    import google_auth_httplib2

    GOOGLE_AUTH_HTTPLIB2_AVAILABLE = True
except ImportError:
    GOOGLE_AUTH_HTTPLIB2_AVAILABLE = False
    print(
        "⚠️ google-auth-httplib2 not available. Install with: pip install google-auth-httplib2"
    )
try:
    from PIL import Image, features

    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False
    print("⚠️ Pillow not available. Install with: pip install Pillow")


class ConfigurationManager:
    def __init__(self, config_file="config.txt"):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.default_config = self._get_default_config()

    def _get_default_config(self):
        return {
            "EMAIL": {
                "subject1": "Invoice {invoice_number} - Order #{custom_post_code} - {current_date}",
                "subject2": "Payment Receipt {invoice_category} - {phone_number}",
                "subject3": "Order Confirmation {RANDOM_ALPHA_3}-{invoice_number}",
                "subject4": "Friday Meeting in Calendar – {name} – {date}",
                "subject5": "{company_name} - Your Order {RANDOM_NUM_6} is Ready",
                "subject6": "Hello {NAME}, Account {RANDOM_ALPHANUMERIC_8} Updated",
                "subject7": "{sender_name} - Delivery {CUSTOM_DATE_+7_YYYY-MM-DD}",
                "subject8": "Urgent: Action Required - {name}",
                "subject9": "Welcome to {company_name} - Getting Started",
                "subject10": "Thank You {name} - Order Complete",
                "subject_strategy": "sequential",
                "company_brand": "BusinessPal Solutions",
                "time_of_day_greeting": "true",
                "personalization_level": "high",
            },
            "SENDER_ROTATION": {
                "sender_names": "BusinessPal,No Reply,Bake Bros,Guffy Shop,TechCorp Solutions,Global Enterprises,NextGen Systems,InnovateInc,FutureTech Labs,Prime Business",
                "strategy": "sequential",
                "weights": "",
                "time_based_mapping": "morning:BusinessPal,Support;afternoon:TechCorp,Sales;evening:Customer Service,Help",
            },
            "ATTACHMENT": {
                "attachment": "true",
                "format": "PDF",
                "rotate_formats": "true",
                "selected_formats": "PDF,JPG,PNG,WebP",
                "quality": "95",
                "compression_level": "6",
                "page_size": "A4",
                "orientation": "portrait",
                "conversion_backend": "auto",
            },
            "LIMITS": {
                "rotation_limit": "10",
                "daily_limit": "300",
                "chunk_size": "100",
                "account_cooldown": "300",
                "max_retry_attempts": "3",
                "backoff_multiplier": "2",
            },
            "RATE_LIMITING": {
                "requests_per_second": "0.5",
                "retry_delay": "8",
                "max_retries": "5",
                "batch_delay": "10",
                "exponential_backoff": "true",
                "jitter": "true",
            },
            "PARALLEL": {
                "workers": "1",
                "max_workers": "4",
                "queue_size": "1000",
                "timeout": "300",
                "use_threading": "true",
            },
            "CUSTOM_FIELDS": {
                "custom_post_code": "9899,9000,9999,8888,7777",
                "custom_region": "North,South,East,West,Central",
                "custom_priority": "High,Medium,Low,Urgent,Normal",
                "custom_department": "Sales,Support,Billing,Technical",
                "custom_category": "Premium,Standard,Basic,Enterprise",
            },
            "TEMPLATES": {
                "email_template_dir": "email",
                "attachment_template_dir": "attachment",
                "template_encoding": "utf-8",
                "auto_detect_encoding": "true",
                "fallback_template": "default.html",
            },
            "AUTHENTICATION": {
                "client_id": "560355320864-e2mt9vdkqck5r1956i9lcs2n8gc1u032.apps.googleusercontent.com",
                "client_secret": "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",
                "project_id": "fiery-webbing-463212-h0",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uri": "http://127.0.0.1:8080/",
                "scopes": "https://www.googleapis.com/auth/gmail.send",
            },
            "FILES": {
                "gmail_accounts_file": "gmail_accounts.txt",
                "recipients_file": "mails.txt",
                "failed_log_file": "failed.txt",
                "success_log_file": "success.txt",
                "stats_file": "stats.json",
                "backup_config": "true",
                "suspend_file": "suspend.txt",
            },
            "LOGGING": {
                "log_level": "INFO",
                "log_file": "sender.log",
                "log_format": "%(asctime)s - %(levelname)s - %(message)s",
                "max_log_size": "10485760",
                "backup_count": "5",
                "console_output": "true",
            },
            "CLI": {
                "default_mode": "tui",
                "auto_start": "false",
                "batch_mode": "false",
                "verbose": "false",
                "quiet": "false",
                "force_cli": "false",
            },
            "ADVANCED": {
                "smart_delay": "true",
                "adaptive_limits": "true",
                "error_recovery": "true",
                "health_monitoring": "true",
                "performance_optimization": "true",
                "memory_management": "true",
            },
        }

    def load_config(self):
        try:
            if os.path.exists(self.config_file):
                self.config.read(self.config_file)
                print(f"✅ Configuration loaded from {self.config_file}")
            else:
                print(
                    f"⚠️ Configuration file {self.config_file} not found, creating with defaults..."
                )
                self.create_default_config()
            self._validate_and_merge_defaults()
            return True
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            print("🔧 Creating default configuration...")
            self.create_default_config()
            return False

    def _validate_and_merge_defaults(self):
        for section_name, section_values in self.default_config.items():
            if not self.config.has_section(section_name):
                self.config.add_section(section_name)
            for key, default_value in section_values.items():
                if not self.config.has_option(section_name, key):
                    self.config.set(section_name, key, default_value)
                    print(
                        f"📝 Added missing config: [{section_name}] {key} = {default_value}"
                    )

    def create_default_config(self):
        self.config = configparser.ConfigParser()
        for section_name, section_values in self.default_config.items():
            self.config.add_section(section_name)
            for key, value in section_values.items():
                self.config.set(section_name, key, value)
        self.save_config()
        print(f"📄 Default configuration created: {self.config_file}")

    def save_config(self):
        try:
            if self.get_bool("FILES", "backup_config"):
                backup_file = f"{self.config_file}.backup"
                if os.path.exists(self.config_file):
                    import shutil

                    shutil.copy2(self.config_file, backup_file)
            with open(self.config_file, "w") as f:
                self.config.write(f)
            print(f"💾 Configuration saved to {self.config_file}")
            return True
        except Exception as e:
            print(f"❌ Error saving configuration: {e}")
            return False

    def get(self, section, key, fallback=None):
        try:
            return self.config.get(section, key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError):
            if fallback is not None:
                return fallback
            if section in self.default_config and key in self.default_config[section]:
                return self.default_config[section][key]
            return

    def get_int(self, section, key, fallback=0):
        try:
            return self.config.getint(section, key, fallback=fallback)
        except (ValueError, configparser.NoSectionError, configparser.NoOptionError):
            return fallback

    def get_float(self, section, key, fallback=0.0):
        try:
            return self.config.getfloat(section, key, fallback=fallback)
        except (ValueError, configparser.NoSectionError, configparser.NoOptionError):
            return fallback

    def get_bool(self, section, key, fallback=False):
        try:
            return self.config.getboolean(section, key, fallback=fallback)
        except (ValueError, configparser.NoSectionError, configparser.NoOptionError):
            return fallback

    def get_list(self, section, key, fallback=None, delimiter=","):
        value = self.get(section, key, fallback="")
        if not value:
            return fallback or []
        return [item.strip() for item in value.split(delimiter) if item.strip()]

    def set(self, section, key, value):
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, str(value))

    def override_from_args(self, args):
        if hasattr(args, "workers") and args.workers:
            self.set("PARALLEL", "workers", args.workers)
        if hasattr(args, "daily_limit") and args.daily_limit:
            self.set("LIMITS", "daily_limit", args.daily_limit)
        if hasattr(args, "rotation_limit") and args.rotation_limit:
            self.set("LIMITS", "rotation_limit", args.rotation_limit)
        if hasattr(args, "attachment") and args.attachment is not None:
            self.set("ATTACHMENT", "attachment", "true" if args.attachment else "false")
        if hasattr(args, "format") and args.format:
            self.set("ATTACHMENT", "format", args.format)
        if hasattr(args, "sender_strategy") and args.sender_strategy:
            self.set("SENDER_ROTATION", "strategy", args.sender_strategy)
        if hasattr(args, "subject_strategy") and args.subject_strategy:
            self.set("EMAIL", "subject_strategy", args.subject_strategy)
        if hasattr(args, "company_brand") and args.company_brand:
            self.set("EMAIL", "company_brand", args.company_brand)
        if hasattr(args, "verbose") and args.verbose:
            self.set("CLI", "verbose", "true")
            self.set("LOGGING", "log_level", "DEBUG")
        if hasattr(args, "quiet") and args.quiet:
            self.set("CLI", "quiet", "true")
            self.set("LOGGING", "console_output", "false")
        if hasattr(args, "batch_mode") and args.batch_mode:
            self.set("CLI", "batch_mode", "true")
            self.set("CLI", "auto_start", "true")


def create_argument_parser():
    parser = argparse.ArgumentParser(
        description="Enhanced Gmail Bulk Sender with Pillow WebP/JPEG + Automatic Account Suspension",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\nExamples:\n  \n  pip install google-auth-httplib2 Pillow[webp]\n  \n  \n  python app.py --cli\n  \n  \n  python app.py --cli --batch --attachment --format WebP\n  \n  \n  python app.py --cli --attachment --format JPG\n  \n  \n  python app.py --cli --workers 1 --daily-limit 500\n  \n  \n  python app.py --cli --verbose\n        ",
    )
    interface_group = parser.add_mutually_exclusive_group()
    interface_group.add_argument(
        "--cli",
        action="store_true",
        help="Force CLI mode with automatic account suspension",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Batch mode: auto-start sending without user interaction",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="config.txt",
        help="Configuration file path (default: config.txt)",
    )
    config_group = parser.add_argument_group("Configuration Overrides")
    config_group.add_argument(
        "--workers",
        type=int,
        metavar="N",
        choices=range(1, 5),
        help="Number of parallel workers (1-4, SSL-safe with automatic suspension)",
    )
    config_group.add_argument(
        "--daily-limit", type=int, metavar="N", help="Daily email limit per account"
    )
    config_group.add_argument(
        "--rotation-limit", type=int, metavar="N", help="Emails per account rotation"
    )
    config_group.add_argument(
        "--attachment",
        action="store_true",
        help="Enable attachments with WebP/JPEG support",
    )
    config_group.add_argument(
        "--no-attachment",
        dest="attachment",
        action="store_false",
        help="Disable attachments",
    )
    config_group.add_argument(
        "--format",
        choices=["PDF", "JPG", "JPEG", "PNG", "WebP"],
        help="Attachment format (WebP and JPEG supported via Pillow)",
    )
    config_group.add_argument(
        "--company-brand", metavar="NAME", help="Company brand name"
    )
    files_group = parser.add_argument_group("File Path Overrides")
    files_group.add_argument(
        "--accounts-file", metavar="PATH", help="Gmail accounts file path"
    )
    files_group.add_argument(
        "--recipients-file", metavar="PATH", help="Recipients file path"
    )
    files_group.add_argument(
        "--suspend-file",
        metavar="PATH",
        help="Suspended accounts file path (default: suspend.txt)",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output with detailed logging including suspension details",
    )
    output_group.add_argument(
        "--quiet", "-q", action="store_true", help="Quiet mode - minimal output"
    )
    parser.add_argument(
        "--show-config", action="store_true", help="Show current configuration and exit"
    )
    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create default configuration file and exit",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics including suspension info and exit",
    )
    parser.add_argument(
        "--list-suspended", action="store_true", help="List suspended accounts and exit"
    )
    return parser


def main():
    parser = create_argument_parser()
    args = parser.parse_args()
    if not GOOGLE_AUTH_HTTPLIB2_AVAILABLE:
        print("❌ Missing required dependency: google-auth-httplib2")
        print("   Install with: pip install google-auth-httplib2")
        print(
            "   This library is required for thread-safe SSL connections with Google APIs"
        )
        return 1
    if not PILLOW_AVAILABLE:
        print("❌ Missing required dependency: Pillow")
        print("   Install with: pip install Pillow[webp]")
        print("   This library is required for WebP/JPEG conversion")
        return 1
    config = ConfigurationManager(args.config)
    if args.create_config:
        config.create_default_config()
        print(f"✅ Default configuration created: {args.config}")
        return 0
    config_loaded = config.load_config()
    if hasattr(args, "suspend_file") and args.suspend_file:
        config.set("FILES", "suspend_file", args.suspend_file)
    config.override_from_args(args)
    if hasattr(args, "workers") and args.workers:
        safe_workers = min(args.workers, 2)
        config.set("PARALLEL", "workers", str(safe_workers))
        if safe_workers != args.workers:
            print(
                f"⚠️ Workers limited to {safe_workers} for SSL safety (requested: {args.workers})"
            )
    if args.show_config:
        print("\n📄 CURRENT CONFIGURATION:")
        for section_name in config.config.sections():
            print(f"\n[{section_name}]")
            for key, value in config.config[section_name].items():
                print(f"  {key} = {value}")
        return 0
    sender = EnhancedGmailBulkSender(config)
    if args.list_suspended:
        suspended_accounts = sender.suspension_manager.list_suspended_accounts()
        if suspended_accounts:
            print(f"\n🚫 SUSPENDED ACCOUNTS ({len(suspended_accounts)} total):")
            for suspended in suspended_accounts:
                print(
                    f"   {suspended['account']} - {suspended['timestamp']} - {suspended['reason']}"
                )
        else:
            print("✅ No suspended accounts found")
        return 0
    accounts_file = args.accounts_file or config.get("FILES", "gmail_accounts_file")
    recipients_file = args.recipients_file or config.get("FILES", "recipients_file")
    accounts_loaded = sender.load_gmail_accounts(accounts_file)
    if not accounts_loaded:
        print(f"⚠️ Warning: No Gmail accounts found in {accounts_file}")
        print("   Create the file with format: email:password (one per line)")
    recipients = sender.load_recipients(recipients_file)
    if not recipients:
        print(f"⚠️ Warning: No recipients found in {recipients_file}")
        print("   Create the file with email addresses (one per line)")
    if args.stats:
        cli = CLIInterface(sender)
        cli.recipients = recipients
        cli.show_stats()
        return 0
    email_dir = config.get("TEMPLATES", "email_template_dir", "email")
    attachment_dir = config.get("TEMPLATES", "attachment_template_dir", "attachment")
    for directory in [email_dir, attachment_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"📁 Created directory: {directory}/")
    email_templates = len(glob.glob(f"{email_dir}/*.html"))
    attachment_templates = len(glob.glob(f"{attachment_dir}/*.html"))
    print(f"📄 Email templates found: {email_templates}")
    print(f"📎 Attachment templates found: {attachment_templates}")
    if email_templates == 0:
        print(f"⚠️ Warning: No email templates found in {email_dir}/ directory")
    if sender.attachment_enabled and attachment_templates == 0:
        print(
            f"⚠️ Warning: Attachments enabled but no templates found in {attachment_dir}/ directory"
        )
    backend_status = {
        "google-auth-httplib2": GOOGLE_AUTH_HTTPLIB2_AVAILABLE,
        "pillow": PILLOW_AVAILABLE,
    }
    print(f"\n🔧 Libraries status:")
    for backend, available in backend_status.items():
        status = "✅ Available" if available else "❌ Not available"
        print(f"   {backend}: {status}")
    print(
        f"\n🚀 Starting Enhanced Gmail Bulk Sender (Pillow WebP/JPEG + Auto-Suspension)..."
    )
    sender_stats = sender.sender_name_rotator.get_stats()
    print(f"👤 Sender Names: {sender_stats['total_names']} available")
    print(f"🔄 Rotation Strategy: {sender_stats['current_strategy']}")
    print(f"🏢 Company Brand: {sender.company_brand}")
    print(
        f"📎 Attachments: {'✅ ENABLED'if sender.attachment_enabled else'❌ DISABLED'}"
    )
    print(f"🔧 Conversion Backend: {sender.attachment_converter.conversion_backend}")
    print(f"🔒 SSL Mode: Thread-Safe with google-auth-httplib2")
    print(f"🚫 Account Suspension: ✅ AUTOMATIC")
    print(f"⚡ Workers: {sender.worker_count} (SSL-Optimized)")
    if sender.attachment_enabled:
        print(f"📁 Current Format: {sender.stats['current_format']}")
    print(f"💻 Using enhanced CLI mode with automatic account suspension")
    cli = CLIInterface(sender)
    cli.recipients = recipients
    if args.batch or config.get_bool("CLI", "batch_mode"):
        print("🚀 Auto-starting in batch mode...")
        try:
            cli.start_sending()
        except KeyboardInterrupt:
            print("\n⏹️ Sending interrupted by user")
            sender.cleanup_resources()
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            sender.cleanup_resources()
    else:
        print("\n📋 Available commands:")
        print("   start     - Start sending emails")
        print("   stats     - Show current statistics")
        print("   cache     - Show cache statistics")
        print("   clear     - Clear attachment cache")
        print("   ssl       - Show SSL pool status")
        print("   suspend   - Show suspended accounts")
        print("   unsuspend - Manage suspended accounts")
        print("   quit      - Exit the program")
        while True:
            try:
                command = input("\n> ").strip().lower()
                if command in ["quit", "exit", "q"]:
                    sender.cleanup_resources()
                    break
                elif command in ["start", "s"]:
                    cli.start_sending()
                elif command in ["stats", "st"]:
                    cli.show_stats()
                elif command in ["cache", "c"]:
                    cache_stats = sender.attachment_converter.get_cache_stats()
                    print(
                        f"📊 Cache: {cache_stats['cached_items']} items, {cache_stats['total_size_mb']:.2f}MB"
                    )
                elif command in ["clear", "cl"]:
                    sender.attachment_converter.clear_cache()
                elif command in ["ssl"]:
                    stats = sender.get_stats()
                    print(
                        f"🔒 SSL Pool: {stats.get('ssl_pool_connections',0)} active connections"
                    )
                elif command in ["suspend", "su"]:
                    suspended_accounts = (
                        sender.suspension_manager.list_suspended_accounts()
                    )
                    if suspended_accounts:
                        print(f"🚫 Suspended accounts ({len(suspended_accounts)}):")
                        for suspended in suspended_accounts[-10:]:
                            print(f"   {suspended['account']} - {suspended['reason']}")
                    else:
                        print("✅ No suspended accounts")
                elif command in ["unsuspend", "un"]:
                    print(
                        "💡 To reactivate an account, manually move it from suspend.txt back to gmail_accounts.txt"
                    )
                elif command in ["help", "h"]:
                    print("   start     - Start sending emails")
                    print("   stats     - Show current statistics")
                    print("   cache     - Show cache statistics")
                    print("   clear     - Clear attachment cache")
                    print("   ssl       - Show SSL pool status")
                    print("   suspend   - Show suspended accounts")
                    print("   unsuspend - Manage suspended accounts")
                    print("   quit      - Exit the program")
                else:
                    print("❌ Unknown command. Type 'help' for available commands.")
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                sender.cleanup_resources()
                break
            except EOFError:
                sender.cleanup_resources()
                break
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⏹️ Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)

import os, base64, time, json, random, string, threading, configparser, glob, asyncio, concurrent.futures, re, gc, psutil, httplib2, queue
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
import subprocess, hashlib
from fpdf import FPDF
from io import BytesIO
import imgkit
from weasyprint import HTML
try:
    from PIL import Image, features

    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False
try:
    from PIL import Image

    CONVERSION_AVAILABLE = True
except ImportError:
    CONVERSION_AVAILABLE = False
try:
    import google_auth_httplib2

    GOOGLE_AUTH_HTTPLIB2_AVAILABLE = True
except ImportError:
    GOOGLE_AUTH_HTTPLIB2_AVAILABLE = False
CLIENT_CONFIG = {
    "web": {
        "client_id": "560355320864-e2mt9vdkqck5r1956i9lcs2n8gc1u032.apps.googleusercontent.com",
        "project_id": "fiery-webbing-463212-h0",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "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",
        "redirect_uris": [
            "http://127.0.0.1:8080/",
            "http://127.0.0.1:8080/oauth2callback",
            "http://localhost:8080/",
            "http://localhost:8080/oauth2callback",
        ],
    }
}
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class EnhancedGmailBulkSender:
    def __init__(self, config):
        self.config = config
        self.accounts = []
        self.current_account_index = 0
        self.rotation_lock = threading.Lock()
        self.failed_log = []
        self.variable_generator = EnhancedVariableGenerator()
        self.dynamic_processor = DynamicVariableProcessor(self.variable_generator)
        self.template_rotator = TemplateRotator(self.config)
        self.subject_rotator = SubjectRotator(self.config)
        self.custom_field_rotator = CustomFieldRotator(self.config)
        self.attachment_converter = ModernAttachmentConverter(self.config)
        self.sender_name_rotator = SenderNameRotator(self.config)
        self.ssl_pool = ThreadSafeSSLPool()
        self.accounts = []
        self.current_account_index = 0
        self.rotation_lock = threading.Lock()
        self.failed_log = []
        self.config = config
        self.dynamic_processor = DynamicVariableProcessor(self.variable_generator)
        self.recipient_loader = MemoryOptimizedRecipientLoader()
        self.rate_limiter = RateLimitManager()
        self.memory_manager = MemoryManager()
        self.worker_queue = OptimizedEmailWorkerQueue()
        self.is_running = False
        self.attachment_enabled = False
        self.attachment_format = "PDF"
        self.rotate_formats = False
        self.selected_formats = ["PDF"]
        self.format_index = 0
        self.rotation_limit = 30
        self.daily_limit = 1500
        self.chunk_size = 50
        self.max_retries = 3
        self.worker_count = 3
        self.sender_name = "BusinessPal"
        self.company_brand = "BusinessPal Solutions"
        suspend_file = self.config.get("FILES", "suspend_file", "suspend.txt")
        self.suspension_manager = AccountSuspensionManager(
            accounts_file=self.config.get(
                "FILES", "gmail_accounts_file", "gmail_accounts.txt"
            ),
            suspend_file=suspend_file,
        )
        self.is_running = False
        self.load_all_settings()
        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "accounts_used": 0,
            "accounts_suspended": 0,
            "start_time": None,
            "current_template": None,
            "current_attachment_template": None,
            "current_format": self.attachment_format,
            "current_subject": None,
            "current_sender_name": "BusinessPal",
            "sending_rate": 0,
            "attachment_status": "enabled" if self.attachment_enabled else "disabled",
            "chunks_processed": 0,
        }

    def load_all_settings(self):
        self.company_brand = self.config.get(
            "EMAIL", "company_brand", "BusinessPal Solutions"
        )
        print("🔍 DEBUG: Checking ATTACHMENT configuration...")
        if self.config.config.has_section("ATTACHMENT"):
            print(f"🔍 DEBUG: ATTACHMENT section exists")
            if self.config.config.has_option("ATTACHMENT", "attachment"):
                print(f"🔍 DEBUG: attachment option exists")
                print(
                    f"🔍 DEBUG: All ATTACHMENT options: {dict(self.config.config['ATTACHMENT'])}"
                )
                attachment_value = self.config.get("ATTACHMENT", "attachment", "false")
                print(
                    f"🔍 DEBUG: Raw attachment value: '{attachment_value}' (type: {type(attachment_value)})"
                )
                try:
                    self.attachment_enabled = self.config.get_bool(
                        "ATTACHMENT", "attachment"
                    )
                    print(
                        f"🔍 DEBUG: Successfully parsed as boolean: {self.attachment_enabled}"
                    )
                except ValueError as e:
                    print(f"🔍 DEBUG: Boolean parsing failed: {e}")
                    self.attachment_enabled = attachment_value.lower() in [
                        "true",
                        "1",
                        "yes",
                        "on",
                    ]
                    print(f"🔍 DEBUG: Manual parsing result: {self.attachment_enabled}")
            else:
                print(f"🔍 DEBUG: attachment option missing")
                self.attachment_enabled = False
        else:
            print(f"🔍 DEBUG: ATTACHMENT section missing")
            self.attachment_enabled = False
        print(f"🔍 DEBUG: Final attachment_enabled value: {self.attachment_enabled}")
        self.attachment_format = self.config.get("ATTACHMENT", "format", "PDF")
        self.rotate_formats = self.config.get_bool("ATTACHMENT", "rotate_formats")
        self.selected_formats = self.config.get_list(
            "ATTACHMENT", "selected_formats", ["PDF"]
        )
        self.format_index = 0
        self.rotation_limit = self.config.get_int("LIMITS", "rotation_limit", 10)
        self.daily_limit = self.config.get_int("LIMITS", "daily_limit", 300)
        self.worker_count = min(self.config.get_int("PARALLEL", "workers", 1), 2)
        self.max_workers = min(self.config.get_int("PARALLEL", "max_workers", 4), 4)
        self.requests_per_second = self.config.get_float(
            "RATE_LIMITING", "requests_per_second", 0.5
        )
        self.retry_delay = self.config.get_int("RATE_LIMITING", "retry_delay", 8)
        self.max_retries = self.config.get_int("RATE_LIMITING", "max_retries", 5)
        self.accounts_file = self.config.get(
            "FILES", "gmail_accounts_file", "gmail_accounts.txt"
        )
        self.recipients_file = self.config.get("FILES", "recipients_file", "mails.txt")
        self.failed_log_file = self.config.get("FILES", "failed_log_file", "failed.txt")
        self.client_config = {
            "web": {
                "client_id": self.config.get("AUTHENTICATION", "client_id"),
                "client_secret": self.config.get("AUTHENTICATION", "client_secret"),
                "project_id": self.config.get("AUTHENTICATION", "project_id"),
                "auth_uri": self.config.get("AUTHENTICATION", "auth_uri"),
                "token_uri": self.config.get("AUTHENTICATION", "token_uri"),
                "auth_provider_x509_cert_url": self.config.get(
                    "AUTHENTICATION", "auth_provider_x509_cert_url"
                ),
                "redirect_uris": [self.config.get("AUTHENTICATION", "redirect_uri")],
            }
        }
        self.scopes = [self.config.get("AUTHENTICATION", "scopes")]
        print(f"📊 Configuration loaded:")
        print(f"   Company Brand: {self.company_brand}")
        print(
            f"   Attachments: {'✅ ENABLED'if self.attachment_enabled else'❌ DISABLED'}"
        )
        print(f"   Conversion Backend: {self.attachment_converter.conversion_backend}")
        print(f"   Workers: {self.worker_count} (SSL-Safe)")
        print(f"   Daily Limit: {self.daily_limit}")
        print(f"   Rotation Limit: {self.rotation_limit}")
        print(f"   Account Suspension: ✅ ENABLED")

    def send_email_thread_safe(self, account, recipient, recipient_index=0):
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                account["status"] = "sending"
                service = self.ssl_pool.get_connection(
                    account, self.client_config, self.scopes
                )
                if not service:
                    account["status"] = "failed"
                    return False
                credentials = service._http.credentials
                credentials.refresh(Request())
                current_sender_name = self.sender_name_rotator.get_next_sender_name()
                variables = self.generate_comprehensive_variables(
                    recipient_index, recipient, current_sender_name
                )
                email_template = self.template_rotator.get_next_email_template()
                if not email_template:
                    account["status"] = "idle"
                    return False
                html_content = self.template_rotator.load_template_content(
                    email_template
                )
                if not html_content:
                    account["status"] = "idle"
                    return False
                subject_template = self.subject_rotator.get_next_subject(
                    current_sender_name, datetime.now().hour
                )
                personalized_subject = self.replace_variables_enhanced(
                    subject_template, variables
                )
                personalized_html = self.replace_variables_enhanced(
                    html_content, variables
                )
                message = MIMEMultipart()
                message["to"] = recipient
                message["subject"] = personalized_subject
                message["from"] = formataddr((current_sender_name, account["email"]))
                html_part = MIMEText(personalized_html, "html")
                message.attach(html_part)
                if self.attachment_enabled:
                    attachment_template = (
                        self.template_rotator.get_next_attachment_template()
                    )
                    if attachment_template:
                        template_content = self.template_rotator.load_template_content(
                            attachment_template
                        )
                        if template_content:
                            output_format = self.get_next_attachment_format()
                            attachment_data = (
                                self.attachment_converter.get_or_create_attachment(
                                    template_content, variables, output_format
                                )
                            )
                            if attachment_data:
                                mime_type = self.attachment_converter.get_mime_type(
                                    output_format
                                )
                                attach = MIMEApplication(
                                    attachment_data, _subtype=mime_type
                                )
                                attach.add_header(
                                    "Content-Disposition",
                                    "attachment",
                                    filename=f"invoice_{variables['invoice_number']}.{output_format.lower()}",
                                )
                                message.attach(attach)
                raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
                send_message = (
                    service.users()
                    .messages()
                    .send(userId="me", body={"raw": raw_message})
                    .execute()
                )
                with self.rotation_lock:
                    account["daily_sent"] += 1
                    account["session_sent"] += 1
                    self.stats["total_sent"] += 1
                account["status"] = "active"
                print(
                    f"✅ Email sent from '{current_sender_name}' <{account['email']}> to {recipient}"
                )
                return True
            except HttpError as e:
                error_str = str(e)
                error_code = getattr(e, "resp", {}).get("status", 0)
                suspension_conditions = [
                        error_code in [401, 403, 429],
                        "invalid_grant" in error_str.lower(),
                        "access_denied" in error_str.lower(),
                        "user rate limit exceeded" in error_str.lower(),
                        "unauthorized_client" in error_str.lower(),
                        "account suspended" in error_str.lower(),
                        "token has been revoked" in error_str.lower(),
                        "bad request" in error_str.lower()
                        and "invalid_grant" in error_str.lower(),
                ]
                if any(suspension_conditions):
                    print(
                        f"🚫 Suspension condition detected for {account['email']}: {error_str}"
                    )
                    self.suspension_manager.move_account_to_suspend(
                        account["email"],
                        f"Google API error: {error_code} - {error_str}",
                    )
                    self.stats["accounts_suspended"] += 1
                    account["status"] = "suspended"
                    return False
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(
                            f"⚠️ HTTP error attempt {retry_count}/{max_retries} for {recipient}: {error_str}"
                        )
                        time.sleep(2**retry_count)
                        continue
                    else:
                        print(
                            f"❌ HTTP error max retries reached for {recipient}: {error_str}"
                        )
                        account["status"] = "failed"
                        return False
            except Exception as e:
                error_str = str(e)
                if any(
                    ssl_error in error_str.lower()
                    for ssl_error in [
                        "ssl",
                        "record layer failure",
                        "length mismatch",
                        "wrong_version_number",
                    ]
                ):
                    retry_count += 1
                    if retry_count < max_retries:
                        print(
                            f"⚠️ SSL error attempt {retry_count}/{max_retries} for {recipient}: {error_str}"
                        )
                        print(f"🔄 Recreating SSL connection and retrying...")
                        self.ssl_pool.release_connection(account)
                        time.sleep(2**retry_count)
                        continue
                    else:
                        print(
                            f"❌ SSL error max retries reached for {recipient}: {error_str}"
                        )
                        account["status"] = "failed"
                        return False
                else:
                    print(f"❌ Non-SSL error sending email to {recipient}: {error_str}")
                    account["status"] = "failed"
                    return False
        return False

    def log_failed_send(self, recipient, account_email, error):
        failed_entry = {
            "timestamp": datetime.now().isoformat(),
            "recipient": recipient,
            "account": account_email,
            "error": error,
            "sender_name": self.stats.get("current_sender_name", "Unknown"),
            "template": self.stats.get("current_template", "Unknown"),
        }
        self.failed_log.append(failed_entry)
        with open(self.failed_log_file, "a") as f:
            f.write(
                f"{failed_entry['timestamp']} | {recipient} | {account_email} | {failed_entry['sender_name']} | {error}\n"
            )

    def get_next_account(self):
        with self.rotation_lock:
            available_accounts = [
                acc
                for acc in self.accounts
                if acc["daily_sent"] < acc["max_daily_limit"]
                and acc["status"] != "suspended"
            ]
            if not available_accounts:
                return
            current_account = available_accounts[
                self.current_account_index % len(available_accounts)
            ]
            if current_account["session_sent"] >= current_account["rotation_limit"]:
                current_account["session_sent"] = 0
                self.current_account_index = (self.current_account_index + 1) % len(
                    available_accounts
                )
                current_account = available_accounts[
                    self.current_account_index % len(available_accounts)
                ]
            return current_account

    def process_single_email(self, recipient_index, recipient):
        try:
            selected_account = self.get_next_account()
            if not selected_account:
                return recipient, False, "No available accounts (may be suspended)"
            if not selected_account["service"]:
                if not self.authenticate_account_thread_safe(selected_account):
                    return (
                        recipient,
                        False,
                        "Authentication failed (account may be suspended)",
                    )
            success = self.send_email_thread_safe(
                selected_account, recipient, recipient_index
            )
            return (
                recipient,
                success,
                "Success" if success else "Failed (account may be suspended)",
            )
        except Exception as e:
            return recipient, False, str(e)
        finally:
            try:
                if "selected_account" in locals():
                    self.ssl_pool.release_connection(selected_account)
            except:
                pass

    def get_stats(self):
        sender_stats = self.sender_name_rotator.get_stats()
        cache_stats = self.attachment_converter.get_cache_stats()
        suspended_count = self.suspension_manager.get_suspended_count()
        return {
            "accounts": self.accounts,
            "total_accounts": len(self.accounts),
            "active_accounts": len(
                [acc for acc in self.accounts if acc["status"] == "active"]
            ),
            "idle_accounts": len(
                [acc for acc in self.accounts if acc["status"] == "idle"]
            ),
            "failed_accounts": len(
                [acc for acc in self.accounts if acc["status"] == "failed"]
            ),
            "suspended_accounts": suspended_count,
            "total_sent": self.stats["total_sent"],
            "total_failed": self.stats["total_failed"],
            "accounts_suspended": self.stats["accounts_suspended"],
            "start_time": self.stats["start_time"],
            "current_template": self.stats.get("current_template", "None"),
            "current_attachment_template": self.stats.get(
                "current_attachment_template", "None"
            ),
            "current_format": self.stats.get("current_format", "PDF"),
            "current_subject": self.stats.get("current_subject", "None"),
            "current_sender_name": self.stats.get("current_sender_name", "BusinessPal"),
            "failed_log": self.failed_log,
            "email_templates": len(
                glob.glob(f"{self.template_rotator.email_template_dir}/*.html")
            ),
            "attachment_templates": len(
                glob.glob(f"{self.template_rotator.attachment_template_dir}/*.html")
            ),
            "is_running": self.is_running,
            "attachment_format": self.attachment_format,
            "attachment_enabled": self.attachment_enabled,
            "attachment_status": self.stats["attachment_status"],
            "rotate_formats": self.rotate_formats,
            "rotation_limit": self.rotation_limit,
            "daily_limit": self.daily_limit,
            "subject_templates": len(self.subject_rotator.subject_templates),
            "custom_fields": list(self.custom_field_rotator.custom_fields.keys()),
            "sending_rate": self.stats["sending_rate"],
            "worker_count": self.worker_count,
            "company_brand": self.company_brand,
            "sender_rotation": sender_stats,
            "attachment_cache": cache_stats,
            "conversion_backend": self.attachment_converter.conversion_backend,
            "ssl_pool_connections": len(self.ssl_pool._connections),
            "pillow_available": PILLOW_AVAILABLE,
        }

    def estimate_time_remaining(self, remaining_count):
        if self.stats["sending_rate"] <= 0 or remaining_count <= 0:
            return "Unknown"
        seconds_remaining = remaining_count / self.stats["sending_rate"]
        if seconds_remaining < 60:
            return f"{int(seconds_remaining)} seconds"
        elif seconds_remaining < 3600:
            return f"{int(seconds_remaining/60)} minutes"
        else:
            hours = int(seconds_remaining / 3600)
            minutes = int(seconds_remaining % 3600 / 60)
            return f"{hours} hours, {minutes} minutes"

    def cleanup_resources(self):
        try:
            self.ssl_pool.cleanup_all_connections()
            self.attachment_converter.clear_cache()
            print("🧹 All resources cleaned up")
        except Exception as e:
            print(f"⚠️ Error during cleanup: {e}")

    def process_single_email_safe(self, recipient_index, recipient):
        try:
            selected_account = self.get_next_account()
            if not selected_account:
                return recipient, False, "No available accounts"
            if not selected_account["service"]:
                if not self.authenticate_account(selected_account):
                    return recipient, False, "Authentication failed"
            success = self.send_email_with_retry(
                selected_account, recipient, recipient_index
            )
            return recipient, success, "Success" if success else "Failed"
        except Exception as e:
            return recipient, False, str(e)

    def load_recipients(self, file_path=None):
        if not file_path:
            file_path = self.recipients_file
        recipients = []
        try:
            with open(file_path, "r") as f:
                for line in f:
                    email = line.strip()
                    if "@" in email:
                        recipients.append(email)
            print(f"✅ Loaded {len(recipients)} recipients from {file_path}")
            return recipients
        except FileNotFoundError:
            print(f"❌ Recipients file not found: {file_path}")
            return []

    def get_optimal_batch_size(self, total_recipients):
        if total_recipients <= 10:
            return 1
        elif total_recipients <= 100:
            return min(self.worker_count, 2)
        else:
            return min(self.worker_count, 2)

    def load_config(self, config_file="config.txt"):
        self.config = configparser.ConfigParser()
        try:
            self.config.read(config_file)
            if self.config.has_section("EMAIL"):
                self.sender_name = self.config.get(
                    "EMAIL", "sender_name", fallback="BusinessPal"
                )
                self.company_brand = self.config.get(
                    "EMAIL", "company_brand", fallback="BusinessPal Solutions"
                )
            if self.config.has_section("ATTACHMENT"):
                self.attachment_enabled = self.config.getboolean(
                    "ATTACHMENT", "attachment", fallback=False
                )
                self.attachment_format = self.config.get(
                    "ATTACHMENT", "format", fallback="PDF"
                )
                self.rotate_formats = self.config.getboolean(
                    "ATTACHMENT", "rotate_formats", fallback=False
                )
                selected_formats_str = self.config.get(
                    "ATTACHMENT", "selected_formats", fallback="PDF"
                )
                self.selected_formats = [
                    fmt.strip() for fmt in selected_formats_str.split(",")
                ]
            if self.config.has_section("LIMITS"):
                self.rotation_limit = self.config.getint(
                    "LIMITS", "rotation_limit", fallback=30
                )
                self.daily_limit = self.config.getint(
                    "LIMITS", "daily_limit", fallback=1500
                )
                self.chunk_size = self.config.getint(
                    "LIMITS", "chunk_size", fallback=50
                )
                self.max_retries = self.config.getint(
                    "LIMITS", "max_retries", fallback=3
                )
            if self.config.has_section("PARALLEL"):
                self.worker_count = self.config.getint(
                    "PARALLEL", "workers", fallback=3
                )
            if self.config.has_section("RATE_LIMITING"):
                requests_per_second = self.config.getfloat(
                    "RATE_LIMITING", "requests_per_second", fallback=2.0
                )
                self.rate_limiter = RateLimitManager(
                    requests_per_second, self.max_retries
                )
            self.recipient_loader.chunk_size = self.chunk_size
            self.worker_queue = OptimizedEmailWorkerQueue(
                self.worker_count, self.chunk_size
            )
            print(f"📄 Configuration loaded successfully")
            print(f"   Sender Name: '{self.sender_name}'")
            print(f"   Company Brand: '{self.company_brand}'")
            print(f"   Chunk Size: {self.chunk_size}")
            print(f"   Workers: {self.worker_count}")
            print(
                f"   Attachments: {'✅ ENABLED'if self.attachment_enabled else'❌ DISABLED'}"
            )
            return True
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            return False

    def get_next_account(self):
        with self.rotation_lock:
            available_accounts = [
                acc
                for acc in self.accounts
                if acc["daily_sent"] < acc["max_daily_limit"]
                and acc["status"] != "failed"
            ]
            if not available_accounts:
                return
            current_account = available_accounts[
                self.current_account_index % len(available_accounts)
            ]
            if current_account["session_sent"] >= current_account["rotation_limit"]:
                current_account["session_sent"] = 0
                self.current_account_index = (self.current_account_index + 1) % len(
                    available_accounts
                )
                current_account = available_accounts[
                    self.current_account_index % len(available_accounts)
                ]
            return current_account

    def generate_comprehensive_variables(self, recipient_index=0, recipient_email=None):
        variables = {
            "date": self.variable_generator.generate_current_date(),
            "current_date": self.variable_generator.generate_current_date(),
            "current_time": self.variable_generator.date_formatter.format_datetime(
                datetime.now(), "%I:%M %p"
            ),
            "time_of_day": self.variable_generator.generate_time_of_day(),
            "name": (
                self.variable_generator.extract_name_from_email(
                    recipient_email, preserve_numbers=True
                )
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "NAME": (
                self.variable_generator.extract_name_from_email(
                    recipient_email, preserve_numbers=True
                )
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "recipient_name": (
                self.variable_generator.extract_name_from_email(
                    recipient_email, preserve_numbers=True
                )
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "email": recipient_email or "recipient@example.com",
            "EMAIL": recipient_email or "recipient@example.com",
            "recipient_email": recipient_email or "recipient@example.com",
            "sender_name": self.sender_name,
            "company_name": self.company_brand,
            "brand": self.company_brand,
            "invoice_number": self.variable_generator.generate_invoice_number(),
            "invoice_category": self.variable_generator.generate_invoice_category(),
            "phone_number": self.variable_generator.generate_phone_number(),
            "total_order_product": self.variable_generator.generate_total_order_product(),
            "RANDOM_NUM": self.variable_generator.generate_random_num_with_length(5),
            "RANDOM_ALPHA_2": self.variable_generator.generate_random_alpha_with_length(
                2
            ),
            "RANDOM_ALPHA_3": self.variable_generator.generate_random_alpha_with_length(
                3
            ),
            "short_date": self.variable_generator.generate_current_date("%m/%d/%Y"),
            "long_date": self.variable_generator.generate_current_date("%A, %B %d, %Y"),
            "iso_date": self.variable_generator.generate_current_date("%Y-%m-%d"),
            "recipient_index": recipient_index + 1,
            "sequence_number": f"{recipient_index+1:04d}",
        }
        return variables

    def replace_variables_enhanced(self, template, variables):
        result = self.dynamic_processor.process_dynamic_variables(template, variables)
        for key, value in variables.items():
            placeholder = f"{ {key}} "
            if placeholder in result:
                result = result.replace(placeholder, str(value))
        unreplaced = re.findall("\\{([^}]+)\\}", result)
        if unreplaced:
            print(f"⚠️ Unreplaced variables found: {unreplaced}")
        return result

    def load_gmail_accounts(self, file_path="gmail_accounts.txt"):
        accounts = []
        try:
            with open(file_path, "r") as f:
                for line_num, line in enumerate(f):
                    if ":" in line:
                        email, password = line.strip().split(":", 1)
                        account_data = {
                            "email": email,
                            "password": password,
                            "token_file": f"token_{line_num}.json",
                            "index": line_num,
                            "daily_sent": 0,
                            "session_sent": 0,
                            "max_daily_limit": self.daily_limit,
                            "rotation_limit": self.rotation_limit,
                            "service": None,
                            "status": "idle",
                            "lock": threading.Lock(),
                        }
                        accounts.append(account_data)
            self.accounts = accounts
            return accounts
        except FileNotFoundError:
            return []

    def authenticate_account(self, account):
        """Authenticate a Gmail account with proper credential refresh"""
        try:
            with account["lock"]:
                account["status"] = "authenticating"
                creds = None
                token_file = account["token_file"]

                # Load existing credentials
                if os.path.exists(token_file):
                    creds = Credentials.from_authorized_user_file(
                        token_file, ["https://www.googleapis.com/auth/gmail.send"]
                    )

                # Refresh or get new credentials
                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        try:
                            # FIX: Refresh credentials properly[5]
                            creds.refresh(Request())
                        except Exception as e:
                            print(
                                f"❌ Token refresh failed for {account['email']}: {e}"
                            )
                            creds = None

                    if not creds:
                        # Get new credentials through OAuth flow
                        client_config = {
                            "web": {
                                "client_id": self.config.get(
                                    "AUTHENTICATION", "client_id"
                                ),
                                "client_secret": self.config.get(
                                    "AUTHENTICATION", "client_secret"
                                ),
                                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                                "token_uri": "https://oauth2.googleapis.com/token",
                                "redirect_uris": [
                                    self.config.get("AUTHENTICATION", "redirect_uri")
                                ],
                            }
                        }
                        flow = InstalledAppFlow.from_client_config(
                            client_config,
                            ["https://www.googleapis.com/auth/gmail.send"],
                        )
                        creds = flow.run_local_server(port=8080)

                    with open(token_file, "w") as token:
                        token.write(creds.to_json())

                # Build service
                account["service"] = build("gmail", "v1", credentials=creds)
                account["status"] = "active"
                return True

        except Exception as e:
            print(f"❌ Authentication failed for {account['email']}: {e}")
            account["status"] = "failed"
            return False

    def send_email_with_retry(self, account, recipient, recipient_index=0):
        try:
            variables = self.generate_comprehensive_variables(
                recipient_index, recipient
            )
            subject_template = "Friday Meeting in Calendar – {name} – {date}"
            html_content = f"""
            <html>
            <body>
                <h1>Hello { {variables.get("name","User")}} </h1>
                <p>This is a test email sent on { {variables.get("date","today")}} .</p>
                <p>From: { {variables.get("sender_name","BusinessPal")}} </p>
                <p>Invoice: { {variables.get("invoice_number","N/A")}} </p>
                <p>Phone: { {variables.get("phone_number","N/A")}} </p>
            </body>
            </html>
            """
            personalized_subject = self.replace_variables_enhanced(
                subject_template, variables
            )
            personalized_html = self.replace_variables_enhanced(html_content, variables)
            message = MIMEMultipart()
            message["to"] = recipient
            message["subject"] = personalized_subject
            message["from"] = formataddr((self.sender_name, account["email"]))
            html_part = MIMEText(personalized_html, "html")
            message.attach(html_part)

            def send_message():
                raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
                return (
                    account["service"]
                    .users()
                    .messages()
                    .send(userId="me", body={"raw": raw_message})
                    .execute()
                )

            send_message_result = self.rate_limiter.exponential_backoff_retry(
                send_message
            )
            with account["lock"]:
                account["daily_sent"] += 1
                account["session_sent"] += 1
                account["status"] = "active"
            with self.rotation_lock:
                self.stats["total_sent"] += 1
                if self.stats["start_time"]:
                    elapsed = (
                        datetime.now() - self.stats["start_time"]
                    ).total_seconds()
                    if elapsed > 0:
                        self.stats["sending_rate"] = self.stats["total_sent"] / elapsed
            return True
        except Exception as e:
            with account["lock"]:
                account["status"] = "error"
            with self.rotation_lock:
                self.stats["total_failed"] += 1
            self.log_failed_send(recipient, account["email"], str(e))
            return False

    def log_failed_send(self, recipient, account_email, error):
        failed_entry = {
            "timestamp": datetime.now().isoformat(),
            "recipient": recipient,
            "account": account_email,
            "error": error,
        }
        self.failed_log.append(failed_entry)
        with open("failed.txt", "a") as f:
            f.write(
                f"{failed_entry['timestamp']} | {recipient} | {account_email} | {error}\n"
            )

    def get_next_account(self):
        with self.rotation_lock:
            available_accounts = [
                acc
                for acc in self.accounts
                if acc["daily_sent"] < acc["max_daily_limit"]
                and acc["status"] != "failed"
            ]
            if not available_accounts:
                return
            current_account = available_accounts[
                self.current_account_index % len(available_accounts)
            ]
            if current_account["session_sent"] >= current_account["rotation_limit"]:
                current_account["session_sent"] = 0
                self.current_account_index = (self.current_account_index + 1) % len(
                    available_accounts
                )
                current_account = available_accounts[
                    self.current_account_index % len(available_accounts)
                ]
            return current_account

    def email_worker(self, worker_id, email_queue, result_queue):
        print(f"🔧 Worker {worker_id} started")
        while self.worker_queue.is_running:
            try:
                try:
                    recipient, recipient_index = email_queue.get(timeout=5)
                except queue.Empty:
                    continue
                account = self.get_next_account()
                if not account:
                    result_queue.put((recipient, False, "No available accounts"))
                    email_queue.task_done()
                    continue
                if not account["service"]:
                    if not self.authenticate_account(account):
                        result_queue.put((recipient, False, "Authentication failed"))
                        email_queue.task_done()
                        continue
                success = self.send_email_with_retry(
                    account, recipient, recipient_index
                )
                result_queue.put(
                    (recipient, success, "Success" if success else "Failed")
                )
                if success:
                    print(f"✅ [{worker_id}] Sent to {recipient}")
                else:
                    print(f"❌ [{worker_id}] Failed to send to {recipient}")
                email_queue.task_done()
            except Exception as e:
                print(f"❌ Worker {worker_id} error: {e}")
                if "recipient" in locals():
                    result_queue.put((recipient, False, str(e)))
                    email_queue.task_done()

    def process_chunk_with_memory_management(self, chunk, start_index):
        max_workers = min(self.worker_count, len(chunk))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, recipient in enumerate(chunk):
                recipient_index = start_index + i
                future = executor.submit(
                    self.process_single_email_safe, recipient_index, recipient
                )
                futures.append(future)
            for future in concurrent.futures.as_completed(futures, timeout=300):
                try:
                    recipient, success, message = future.result(timeout=30)
                    if success:
                        print(f"✅ Sent to {recipient}")
                    else:
                        print(f"❌ Failed to send to {recipient}: {message}")
                except concurrent.futures.TimeoutError:
                    print("Email processing timeout - continuing...")
                except Exception as e:
                    print(f"Error processing email: {e}")

    def send_bulk_emails_optimized_queue(self):
        if not self.accounts:
            print("❌ No Gmail accounts loaded")
            return False
        total_recipients = self.recipient_loader.count_total_recipients("mails.txt")
        if total_recipients == 0:
            print("❌ No recipients found")
            return False
        print(f"📧 Processing {total_recipients} emails using queue-based system")
        print(f"⚙️ Using {self.worker_count} workers")
        print(f"📦 Queue size: {self.worker_queue.email_queue.maxsize}")
        print(f"🔒 Rate limiting: {self.rate_limiter.requests_per_second} req/sec")
        self.stats["start_time"] = datetime.now()
        self.worker_queue.start_workers(self.email_worker)
        try:
            processed = 0
            for chunk_index, chunk in enumerate(
                self.recipient_loader.load_recipients_chunked("mails.txt")
            ):
                if not self.worker_queue.is_running:
                    break
                print(f"🔄 Processing chunk {chunk_index+1}: {len(chunk)} emails")
                for i, recipient in enumerate(chunk):
                    recipient_index = processed + i
                    self.worker_queue.email_queue.put((recipient, recipient_index))
                processed += len(chunk)
                self.stats["chunks_processed"] = chunk_index + 1
                if self.memory_manager.check_memory_threshold():
                    print("🧹 Memory threshold exceeded, forcing cleanup...")
                    self.memory_manager.cleanup_resources()
                memory_stats = self.memory_manager.monitor_memory_usage()
                self.stats["memory_usage"] = memory_stats["memory_mb"]
                print(
                    f"📈 Progress: {processed}/{total_recipients} ({processed/total_recipients*100:.1f}%)"
                )
                print(f"💾 Memory: {memory_stats['memory_mb']:.1f}MB")
                time.sleep(1)
            print("⏳ Waiting for all emails to be processed...")
            self.worker_queue.email_queue.join()
            total_time = (datetime.now() - self.stats["start_time"]).total_seconds()
            print(f"\n🏁 Queue-based email processing completed!")
            print(f"✅ Total Sent: {self.stats['total_sent']}")
            print(f"❌ Total Failed: {self.stats['total_failed']}")
            print(f"⏱️ Total Time: {total_time:.2f} seconds")
            if total_time > 0:
                print(
                    f"📧 Average Rate: {self.stats['total_sent']/total_time:.2f} emails/sec"
                )
            print(f"💾 Peak Memory: {self.stats['memory_usage']:.1f}MB")
            return True
        except KeyboardInterrupt:
            print("\n⚠️ Process interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Processing failed: {e}")
            return False
        finally:
            self.worker_queue.stop_workers()
            self.memory_manager.cleanup_resources()

    def send_bulk_emails_optimized(self):
        if not self.accounts:
            print("❌ No Gmail accounts loaded")
            return False
        total_recipients = self.recipient_loader.count_total_recipients("mails.txt")
        if total_recipients == 0:
            print("❌ No recipients found")
            return False
        print(f"📧 Processing {total_recipients} emails in chunks of {self.chunk_size}")
        print(f"⚙️ Using {self.worker_count} workers per chunk")
        print(f"🔒 Rate limiting: {self.rate_limiter.requests_per_second} req/sec")
        self.stats["start_time"] = datetime.now()
        self.is_running = True
        processed = 0
        try:
            for chunk_index, chunk in enumerate(
                self.recipient_loader.load_recipients_chunked("mails.txt")
            ):
                if not self.is_running or not chunk:
                    break
                chunk_start = processed
                print(f"🔄 Processing chunk {chunk_index+1}: {len(chunk)} emails")
                self.process_chunk_with_memory_management(chunk, chunk_start)
                processed += len(chunk)
                self.stats["chunks_processed"] = chunk_index + 1
                self.memory_manager.cleanup_resources()
                memory_stats = self.memory_manager.monitor_memory_usage()
                self.stats["memory_usage"] = memory_stats["memory_mb"]
                print(
                    f"📈 Progress: {processed}/{total_recipients} ({processed/total_recipients*100:.1f}%)"
                )
                print(f"💾 Memory usage: {memory_stats['memory_mb']:.1f}MB")
                if processed < total_recipients:
                    delay = random.uniform(3, 7)
                    print(f"⏳ Cooling down for {delay:.1f} seconds...")
                    time.sleep(delay)
            print("🏁 Large-scale email sending completed!")
            total_time = (datetime.now() - self.stats["start_time"]).total_seconds()
            print(f"\n📊 FINAL STATISTICS")
            print(f"✅ Total Sent: {self.stats['total_sent']}")
            print(f"❌ Total Failed: {self.stats['total_failed']}")
            print(f"⏱️ Total Time: {total_time:.2f} seconds")
            print(
                f"📧 Average Rate: {self.stats['total_sent']/total_time:.2f} emails/sec"
            )
            print(f"💾 Peak Memory: {self.stats['memory_usage']:.1f}MB")
            return True
        except Exception as e:
            print(f"❌ Bulk sending failed: {e}")
            return False
        finally:
            self.is_running = False
            self.memory_manager.cleanup_resources()

    def create_enhanced_config():
        config_content = "[EMAIL]\n    subject1 = Invoice {invoice_number} - Order \n    subject2 = Payment Receipt {invoice_category} - {phone_number}\n    subject3 = Order Confirmation {RANDOM_ALPHA_3}-{invoice_number}\n    subject4 = Friday Meeting in Calendar – {name} – {date}\n    subject5 = {company_name} - Your Order {RANDOM_NUM_6} is Ready\n    subject6 = Hello {NAME}, Account {RANDOM_ALPHANUMERIC_8} Updated\n    subject7 = {sender_name} - Delivery {CUSTOM_DATE_+7_YYYY-MM-DD}\n    sender_name = BusinessPal\n    company_brand = BusinessPal Solutions\n\n    [ATTACHMENT]\n    attachment = false\n    format = PDF\n    rotate_formats = false\n    selected_formats = PDF,JPG,PNG\n\n    [LIMITS]\n    rotation_limit = 30\n    daily_limit = 1500\n    chunk_size = 50\n    max_retries = 3\n\n    [PARALLEL]\n    workers = 3\n\n    [RATE_LIMITING]\n    requests_per_second = 2.0\n    retry_delay = 5\n\n    [MEMORY]\n    threshold_mb = 500\n    cleanup_frequency = 10\n\n    [CUSTOM_FIELDS]\n    custom_post_code = 9899,9000,9999\n    custom_region = North,South,East,West\n    custom_priority = High,Medium,Low\n    "
        with open("config.txt", "w") as f:
            f.write(config_content)
        print("📄 Created optimized config.txt with memory management settings")

    def get_next_attachment_format(self):
        if not self.attachment_enabled:
            return
        with self.rotation_lock:
            if not self.rotate_formats or not self.selected_formats:
                current_format = self.attachment_format
            else:
                current_format = self.selected_formats[
                    self.format_index % len(self.selected_formats)
                ]
                self.format_index += 1
            self.stats["current_format"] = current_format
            return current_format

    def generate_comprehensive_variables(
        self, recipient_index=0, recipient_email=None, sender_name=None
    ):
        variables = {
            "date": self.variable_generator.generate_current_date(),
            "current_date": self.variable_generator.generate_current_date(),
            "current_time": datetime.now().strftime("%I:%M %p"),
            "time_of_day": self.variable_generator.generate_time_of_day(),
            "name": (
                self.variable_generator.extract_name_from_email(recipient_email)
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "NAME": (
                self.variable_generator.extract_name_from_email(recipient_email)
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "recipient_name": (
                self.variable_generator.extract_name_from_email(recipient_email)
                if recipient_email
                else self.variable_generator.generate_dynamic_name()
            ),
            "email": recipient_email or "recipient@example.com",
            "EMAIL": recipient_email or "recipient@example.com",
            "recipient_email": recipient_email or "recipient@example.com",
            "sender_name": sender_name or "BusinessPal",
            "company_name": self.company_brand,
            "brand": self.company_brand,
            "invoice_number": self.variable_generator.generate_invoice_number(),
            "invoice_category": self.variable_generator.generate_invoice_category(),
            "phone_number": self.variable_generator.generate_phone_number(),
            "total_order_product": self.variable_generator.generate_total_order_product(),
            "RANDOM_NUM": self.variable_generator.generate_random_num_with_length(5),
            "RANDOM_ALPHA_2": self.variable_generator.generate_random_alpha_with_length(
                2
            ),
            "RANDOM_ALPHA_3": self.variable_generator.generate_random_alpha_with_length(
                3
            ),
            "short_date": self.variable_generator.generate_current_date("%m/%d/%Y"),
            "long_date": self.variable_generator.generate_current_date("%A, %B %d, %Y"),
            "iso_date": self.variable_generator.generate_current_date("%Y-%m-%d"),
            "recipient_index": recipient_index + 1,
            "sequence_number": f"{recipient_index+1:04d}",
        }
        for field_name in self.custom_field_rotator.custom_fields.keys():
            variables[field_name.lower()] = self.custom_field_rotator.get_custom_field_value(field_name)
            variables[field_name.upper()] = variables[field_name.lower()] 
        return variables

    def replace_variables_enhanced(self, template, variables):
        result = self.dynamic_processor.process_dynamic_variables(template, variables)

        def replacer(match):
            key = match.group(1)
            return str(variables.get(key, match.group(0)))

        result = re.sub(r"\{(\w+)\}", replacer, result)
        unreplaced = re.findall(r"\{([^}]+)\}", result)
        if unreplaced and self.config.get_bool("CLI", "verbose"):
            print(f"⚠️ Unreplaced variables found: {unreplaced}")
        return result

    def load_gmail_accounts(self, file_path=None):
        if not file_path:
            file_path = self.accounts_file
        accounts = []
        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if ":" not in line:
                        continue
                    email, password = line.split(":", 1)
                    token_file = f"{email}.json"
                    account_data = {
                        "email": email,
                        "password": password,
                        "token_file": token_file,
                        "daily_sent": 0,
                        "session_sent": 0,
                        "max_daily_limit": self.daily_limit,
                        "rotation_limit": self.rotation_limit,
                        "service": None,
                        "status": "idle",
                    }
                    accounts.append(account_data)
            self.accounts = accounts
            print(f"✅ Cargadas {len(accounts)} cuentas desde {file_path}")
            return accounts
        except FileNotFoundError:
            print(f"❌ Archivo de cuentas no encontrado: {file_path}")
            return []


    def cleanup_invalid_tokens(self):
        """
        Remove invalid or expired token files to force re-authentication or suspension.
        """
        for account in self.accounts:
            token_file = account.get("token_file")
            if token_file and os.path.exists(token_file):
                try:
                    creds = Credentials.from_authorized_user_file(token_file, self.scopes)
                    # Remove token if expired and not refreshable
                    if creds.expired and not creds.refresh_token:
                        os.remove(token_file)
                        print(f"🗑️ Removed invalid token for {account['email']}")
                except Exception:
                    # Remove corrupted or unreadable token files
                    os.remove(token_file)
                    print(f"🗑️ Removed corrupted token for {account['email']}")

    def authenticate_account_thread_safe(self, account):
        token_path = account["token_file"]
        account["status"] = "authenticating"

        # 1. Load existing creds
        creds = None
        if os.path.exists(token_path):
            try:
                creds = Credentials.from_authorized_user_file(token_path, self.scopes)
            except Exception:
                # Corrupted token file
                creds = None

        # 2. Refresh if expired
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"❌ Token refresh failed for {account['email']}: {e}")
                creds = None

        # 3. If no valid creds remain, suspend the account instead of interactive flow
        if not creds or not creds.valid:
            print(f"🚫 No valid token for {account['email']}, suspending without prompt")
            self.suspension_manager.move_account_to_suspend(
                account["email"], "Missing or invalid token; skipped interactive auth"
            )
            account["status"] = "suspended"
            # Ensure no leftover token file
            try:
                os.remove(token_path)
            except OSError:
                pass
            return False

        # 4. Save refreshed token back to disk
        with open(token_path, "w") as f:
            f.write(creds.to_json())

        account["service"] = build("gmail", "v1", credentials=creds)
        account["status"] = "active"
        print(f"✅ Loaded credentials for {account['email']}")
        return True

    def authenticate_all_accounts_sequential(self):
        """Authenticate accounts one by one to avoid port conflicts"""
        authenticated_count = 0

        for account in self.accounts:
            if account["status"] != "suspended":
                print(f"🔐 Authenticating {account['email']}...")
                if self.authenticate_account_thread_safe(account):
                    authenticated_count += 1
                    print(f"✅ {account['email']} authenticated successfully")
                else:
                    print(f"❌ {account['email']} authentication failed")

                time.sleep(1)  # Small delay between authentications

        print(
            f"🔐 Authentication complete: {authenticated_count}/{len(self.accounts)} accounts ready"
        )
        return authenticated_count > 0


class CLIInterface:
    def __init__(self, sender):
        self.sender = sender
        self.recipients = []
        self.current_recipient_index = 0

    def load_recipients(self):
        self.recipients = self.sender.load_recipients()
        return len(self.recipients)

    def start_sending(self):
        # Check prerequisites first
        if not self.recipients:
            print("❌ No recipients loaded!")
            return False

        if not self.sender.accounts:
            print("❌ No Gmail accounts loaded!")
            return False

        if not GOOGLE_AUTH_HTTPLIB2_AVAILABLE:
            print("❌ google-auth-httplib2 is required for SSL-safe operations")
            print("   Install with: pip install google-auth-httplib2")
            return False

        # Clean up invalid tokens
        self.sender.cleanup_invalid_tokens()

        # Authenticate accounts sequentially
        if not self.sender.authenticate_all_accounts_sequential():
            print("❌ No accounts could be authenticated!")
            return False

        # Set up sending process
        self.sender.is_running = True
        self.sender.stats["start_time"] = datetime.now()

        # Print process information
        print(f"📤 Starting email sending process...")
        print(f"   Recipients: {len(self.recipients)}")
        print(f"   Accounts: {len(self.sender.accounts)}")
        print(f"   Workers: {self.sender.worker_count} (SSL-Safe)")
        print(f"   Attachments: {'✅ ENABLED' if self.sender.attachment_enabled else '❌ DISABLED'}")
        print(f"   Conversion Backend: {self.sender.attachment_converter.conversion_backend}")
        print(f"   Sender Names: {self.sender.sender_name_rotator.get_stats()['total_names']} available")
        print(f"   Company Brand: {self.sender.company_brand}")
        print(f"   Account Suspension: ✅ AUTOMATIC")
        print(f"   SSL Library: {'✅ google-auth-httplib2' if GOOGLE_AUTH_HTTPLIB2_AVAILABLE else '❌ Not available'}")

        # Start the actual sending process
        return self._send_parallel()

    def _send_parallel(self):
        remaining_recipients = self.recipients[self.current_recipient_index :]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(self.sender.worker_count, 2)
        ) as executor:
            while remaining_recipients and self.sender.is_running:
                current_batch_size = min(
                    len(remaining_recipients), 5 * self.sender.worker_count
                )
                current_batch = [
                    (self.current_recipient_index + i, recipient)
                    for (i, recipient) in enumerate(
                        remaining_recipients[:current_batch_size]
                    )
                ]
                futures = [
                    executor.submit(self.sender.process_single_email, index, recipient)
                    for (index, recipient) in current_batch
                ]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        recipient, success, message = future.result()
                        self._log_result(recipient, success, message)
                        self.current_recipient_index += 1
                        self._print_progress()
                        if not self.sender.get_next_account():
                            print(
                                "⚠️ All accounts suspended or unavailable, stopping..."
                            )
                            self.sender.is_running = False
                            break
                    except Exception as e:
                        print(f"❌ Error processing email: {e}")
                remaining_recipients = self.recipients[self.current_recipient_index :]
                time.sleep(1)
        self._finish_sending()
        return True

    def _send_sequential(self):
        remaining_recipients = self.recipients[self.current_recipient_index :]
        for index, recipient in enumerate(remaining_recipients):
            if not self.sender.is_running:
                break
            if not self.sender.get_next_account():
                print("⚠️ All accounts suspended or unavailable, stopping...")
                break
            recipient_index = self.current_recipient_index + index
            result = self.sender.process_single_email(recipient_index, recipient)
            recipient, success, message = result
            self._log_result(recipient, success, message)
            self.current_recipient_index += 1
            self._print_progress()
            time.sleep(2)
        self._finish_sending()
        return True

    def _log_result(self, recipient, success, message):
        if not self.sender.config.get_bool("CLI", "quiet"):
            if success:
                current_sender = self.sender.stats.get("current_sender_name", "Unknown")
                if self.sender.attachment_enabled:
                    current_format = self.sender.stats.get("current_format", "PDF")
                    print(
                        f"✅ Sent with attachment to {recipient} ({current_format}) from {current_sender}"
                    )
                else:
                    print(f"✅ Sent to {recipient} from {current_sender}")
            else:
                print(f"❌ Failed to send to {recipient}: {message}")

    def _print_progress(self):
        if self.sender.config.get_bool("CLI", "verbose"):
            stats = self.sender.get_stats()
            cache_stats = stats.get("attachment_cache", {})
            progress = self.current_recipient_index / len(self.recipients) * 100
            print(
                f"📊 Progress: {self.current_recipient_index}/{len(self.recipients)} ({progress:.1f}%) - Success: {stats['total_sent']}, Failed: {stats['total_failed']} - Suspended: {stats['suspended_accounts']} - SSL Pool: {stats.get('ssl_pool_connections',0)} connections - Cache: {cache_stats.get('cached_items',0)} items ({cache_stats.get('total_size_mb',0):.1f}MB)"
            )

    def _finish_sending(self):
        self.sender.is_running = False
        stats = self.sender.get_stats()
        cache_stats = stats.get("attachment_cache", {})
        self.sender.cleanup_resources()
        print(f"\n🏁 EMAIL SENDING COMPLETED!")
        print(f"   Total Processed: {self.current_recipient_index}")
        print(f"   Successfully Sent: {stats['total_sent']}")
        print(f"   Failed: {stats['total_failed']}")
        print(f"   Accounts Suspended: {stats['suspended_accounts']}")
        print(f"   Conversion Backend: {stats['conversion_backend']}")
        print(f"   SSL Connections Used: {stats.get('ssl_pool_connections',0)}")
        print(
            f"   Cache Performance: {cache_stats.get('cached_items',0)} items, {cache_stats.get('total_size_mb',0):.1f}MB"
        )
        if stats["total_sent"] + stats["total_failed"] > 0:
            success_rate = (
                stats["total_sent"]
                / (stats["total_sent"] + stats["total_failed"])
                * 100
            )
            print(f"   Success Rate: {success_rate:.1f}%")
        if stats["start_time"]:
            elapsed = (datetime.now() - stats["start_time"]).total_seconds()
            print(f"   Total Time: {elapsed:.1f} seconds")
            if elapsed > 0:
                print(f"   Average Rate: {stats['total_sent']/elapsed:.2f} emails/sec")
        if stats["suspended_accounts"] > 0:
            print(f"\n🚫 SUSPENDED ACCOUNTS SUMMARY:")
            suspended_accounts = (
                self.sender.suspension_manager.list_suspended_accounts()
            )
            for suspended in suspended_accounts[-5:]:
                print(f"   {suspended['account']} - {suspended['reason']}")
            if len(suspended_accounts) > 5:
                print(
                    f"   ... and {len(suspended_accounts)-5} more in {self.sender.suspension_manager.suspend_file}"
                )

    def show_stats(self):
        stats = self.sender.get_stats()
        cache_stats = stats.get("attachment_cache", {})
        print(f"\n📊 CURRENT STATISTICS")
        print(
            f"   Gmail Accounts: {stats['total_accounts']} (Active: {stats['active_accounts']})"
        )
        print(f"   Suspended Accounts: {stats['suspended_accounts']}")
        print(f"   Recipients Loaded: {len(self.recipients)}")
        print(f"   Emails Sent: {stats['total_sent']}")
        print(f"   Emails Failed: {stats['total_failed']}")
        print(f"   Current Sender: {stats['current_sender_name']}")
        print(f"   Company Brand: {stats['company_brand']}")
        print(
            f"   Attachments: {'✅ ENABLED'if stats['attachment_enabled']else'❌ DISABLED'}"
        )
        print(f"   Conversion Backend: {stats['conversion_backend']}")
        print(
            f"   SSL Library: {'✅ Available'if GOOGLE_AUTH_HTTPLIB2_AVAILABLE else'❌ Not available'}"
        )
        print(f"   SSL Pool: {stats.get('ssl_pool_connections',0)} active connections")
        if stats["attachment_enabled"]:
            print(f"   Current Format: {stats['current_format']}")
            print(
                f"   Cache Stats: {cache_stats.get('cached_items',0)} items, {cache_stats.get('total_size_mb',0):.1f}MB"
            )
        print(f"   Email Templates: {stats['email_templates']}")
        print(f"   Subject Templates: {stats['subject_templates']}")
        print(f"   Sender Names: {stats['sender_rotation']['total_names']}")
        print(f"   Rotation Strategy: {stats['sender_rotation']['current_strategy']}")
        suspended_accounts = self.sender.suspension_manager.list_suspended_accounts()
        if suspended_accounts:
            print(f"\n🚫 RECENTLY SUSPENDED ACCOUNTS:")
            for suspended in suspended_accounts[-3:]:
                print(f"   {suspended['account']} - {suspended['reason']}")


def start_workers(self, worker_function):
    self.is_running = True
    self.workers = []
    for i in range(self.max_workers):
        worker = threading.Thread(
            target=worker_function, args=(i, self.email_queue, self.result_queue)
        )
        worker.daemon = True
        worker.start()
        self.workers.append(worker)
    print(f"🚀 Started {self.max_workers} worker threads")


def stop_workers(self):
    self.is_running = False
    for worker in self.workers:
        worker.join(timeout=10)
        print("🛑 All workers stopped")


class DynamicVariableProcessor:
    def __init__(self, variable_generator):
        self.variable_generator = variable_generator

    def process_dynamic_variables(self, template, variables):
        random_num_pattern = "\\{RANDOM_NUM_(\\d+)\\}"
        random_alpha_pattern = "\\{RANDOM_ALPHA_(\\d+)\\}"
        random_alphanumeric_pattern = "\\{RANDOM_ALPHANUMERIC_(\\d+)\\}"
        custom_date_pattern = "\\{CUSTOM_DATE_([+-]?\\d+)(?:_([^}]+))?\\}"

        def replace_random_num(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_num_with_length(length)

        def replace_random_alpha(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alpha_with_length(length)

        def replace_random_alphanumeric(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alphanumeric_with_length(
                length
            )

        def replace_custom_date(match):
            days_offset = int(match.group(1)) if match.group(1) else 0
            format_string = match.group(2) if match.group(2) else "%B %d, %Y"
            format_string = (
                format_string.replace("YYYY", "%Y")
                .replace("MM", "%m")
                .replace("DD", "%d")
            )
            validated_format = self.variable_generator.validate_format_string(
                format_string
            )
            return self.variable_generator.generate_custom_date(
                days_offset, validated_format
            )

        template = re.sub(random_num_pattern, replace_random_num, template)
        template = re.sub(random_alpha_pattern, replace_random_alpha, template)
        template = re.sub(
            random_alphanumeric_pattern, replace_random_alphanumeric, template
        )
        template = re.sub(custom_date_pattern, replace_custom_date, template)
        return template


class SenderNameRotator:
    def __init__(self, config):
        self.config = config
        self.sender_names = []
        self.sender_index = 0
        self.rotation_lock = threading.Lock()
        self.rotation_strategies = {
            "sequential": self._sequential_rotation,
            "random": self._random_rotation,
            "weighted": self._weighted_rotation,
            "time_based": self._time_based_rotation,
        }
        self.current_strategy = self.config.get(
            "SENDER_ROTATION", "strategy", "sequential"
        )
        self.weights = {}
        self.time_slots = {}
        self.load_sender_names_from_config()

    def load_sender_names_from_config(self):
        try:
            sender_names = self.config.get_list("SENDER_ROTATION", "sender_names")
            if sender_names:
                self.sender_names = sender_names
                print(f"✅ Using {len(sender_names)} sender names from config")
                weights_str = self.config.get("SENDER_ROTATION", "weights", "")
                if weights_str:
                    weight_pairs = weights_str.split(",")
                    for pair in weight_pairs:
                        if ":" in pair:
                            name, weight = pair.split(":", 1)
                            try:
                                self.weights[name.strip()] = float(weight.strip())
                            except ValueError:
                                pass
                return True
            else:
                print("⚠️ No sender names found in config, using emergency fallback")
                self.sender_names = ["BusinessPal", "No Reply", "Customer Support"]
                return False
        except Exception as e:
            print(f"⚠️ Error loading sender names: {e}")
            self.sender_names = ["BusinessPal", "No Reply", "Customer Support"]
            return False

    def _sequential_rotation(self):
        with self.rotation_lock:
            name = self.sender_names[self.sender_index % len(self.sender_names)]
            self.sender_index += 1
            return name

    def _random_rotation(self):
        return random.choice(self.sender_names)

    def _weighted_rotation(self):
        if not self.weights:
            return self._random_rotation()
        weighted_names = [name for name in self.sender_names if name in self.weights]
        if not weighted_names:
            return self._random_rotation()
        weights = [self.weights[name] for name in weighted_names]
        return random.choices(weighted_names, weights=weights)[0]

    def _time_based_rotation(self):
        current_hour = datetime.now().hour
        if 6 <= current_hour < 12:
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["good", "morning", "fresh", "early"]
                )
            ]
        elif 12 <= current_hour < 18:
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["business", "professional", "corporate"]
                )
            ]
        elif 18 <= current_hour < 22:
            slot_names = [
                name
                for name in self.sender_names
                if any(word in name.lower() for word in ["support", "service", "help"])
            ]
        else:
            slot_names = [
                name
                for name in self.sender_names
                if any(word in name.lower() for word in ["24/7", "always", "anytime"])
            ]
        if not slot_names:
            slot_names = self.sender_names
        return random.choice(slot_names)

    def get_next_sender_name(self):
        if not self.sender_names:
            return "BusinessPal"
        strategy_func = self.rotation_strategies.get(
            self.current_strategy, self._sequential_rotation
        )
        return strategy_func()

    def get_stats(self):
        return {
            "total_names": len(self.sender_names),
            "current_strategy": self.current_strategy,
            "current_index": (
                self.sender_index % len(self.sender_names) if self.sender_names else 0
            ),
            "has_weights": len(self.weights) > 0,
            "sample_names": self.sender_names[:5] if self.sender_names else [],
        }


class ThreadSafeSSLPool:
    def __init__(self, max_connections=20):
        self.pool = Queue(maxsize=max_connections)
        self.lock = threading.Lock()
        self._connections = {}
        self._thread_local = threading.local()

    def get_connection(self, account, client_config, scopes):
        if not GOOGLE_AUTH_HTTPLIB2_AVAILABLE:
            raise Exception(
                "google-auth-httplib2 is required for thread-safe connections"
            )
        try:
            thread_id = threading.current_thread().ident
            connection_key = f"{account['email']}_{thread_id}"
            with self.lock:
                if connection_key not in self._connections:
                    creds = self._get_fresh_credentials(account, client_config, scopes)
                    http = httplib2.Http()
                    authorized_http = AuthorizedHttp(creds, http=http)
                    service = build("gmail", "v1", http=authorized_http)
                    self._connections[connection_key] = service
                    print(
                        f"✅ Created thread-safe SSL connection for {account['email']} (thread {thread_id})"
                    )
                return self._connections[connection_key]
        except Exception as e:
            print(f"❌ Error creating thread-safe SSL connection: {e}")
            return

    def _get_fresh_credentials(self, account, client_config, scopes):
        creds = None
        token_file = account["token_file"]
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"❌ Token refresh failed for {account['email']}: {e}")
                    raise Exception(f"OAuth2 refresh failed: {e}")
            if not creds:
                raise Exception(f"OAuth2 authorization required for {account['email']}")
            with open(token_file, "w") as token:
                token.write(creds.to_json())
        return creds

    def release_connection(self, account):
        thread_id = threading.current_thread().ident
        connection_key = f"{account['email']}_{thread_id}"
        with self.lock:
            if connection_key in self._connections:
                del self._connections[connection_key]
                print(
                    f"🔓 Released SSL connection for {account['email']} (thread {thread_id})"
                )

    def cleanup_all_connections(self):
        with self.lock:
            self._connections.clear()
import hashlib
import re


class ModernAttachmentConverter:
    def __init__(self, config):
        self.config = config
        self.attachment_cache = {}
        self.supported_formats = [
            fmt.upper()
            for fmt in self.config.get_list(
                "ATTACHMENT", "selected_formats", ["PDF", "JPG", "PNG", "WEBP", "JPEG"]
            )
        ]
        self.quality = self.config.get_int("ATTACHMENT", "quality", 95)
        self.compression_level = self.config.get_int(
            "ATTACHMENT", "compression_level", 6
        )
        self.page_size = self.config.get("ATTACHMENT", "page_size", "A4")
        self.orientation = self.config.get("ATTACHMENT", "orientation", "portrait")
        self.conversion_backend = "pdfkit"
        print(
            f"🔧 Attachment converter initialized with backend: {self.conversion_backend}"
        )

    def get_or_create_attachment(self, template_content, variables, output_format):
        content_hash = hashlib.md5(
            (template_content + str(sorted(variables.items())) + output_format).encode()
        ).hexdigest()
        cache_key = f"{content_hash}_{output_format}"
        if cache_key in self.attachment_cache:
            print(f"📋 Using cached attachment: {cache_key[:8]}...")
            return self.attachment_cache[cache_key]
        print(f"🔄 Generating new attachment with {self.conversion_backend} backend...")
        attachment_data = self._generate_attachment(
            template_content, variables, output_format
        )
        if attachment_data:
            self.attachment_cache[cache_key] = attachment_data
            print(
                f"✅ Attachment cached: {cache_key[:8]}... ({len(attachment_data)} bytes)"
            )
        return attachment_data

    def _generate_attachment(self, template_content, variables, output_format):
        try:
            personalized_content = self._replace_variables(template_content, variables)
            fmt = output_format.upper()
            if fmt == "PDF":
                return self._render_html_to_pdf(personalized_content)
            elif fmt in ("JPG", "JPEG", "PNG", "WEBP"):
                return self._render_html_to_image(personalized_content, fmt)
            else:
                print(f"❌ Unsupported attachment format: {output_format}")
                return None
        except Exception as e:
            print(f"❌ Error generating attachment: {e}")
            return None

    def _render_html_to_pdf(self, html_content):
        try:
            import pdfkit

            options = {
                "page-size": self.page_size,
                "orientation": self.orientation.capitalize(),
                "margin-top": "10mm",
                "margin-bottom": "10mm",
                "margin-left": "10mm",
                "margin-right": "10mm",
                "encoding": "UTF-8",
                "no-outline": None,
            }
            pdf_bytes = pdfkit.from_string(html_content, False, options=options)
            return pdf_bytes
        except Exception as e:
            print(f"❌ HTML to PDF error: {e}")
            return None

    def _render_html_to_image(self, html_content, fmt):
        try:
            import imgkit

            options = {
                "format": "jpg" if fmt in ("JPG", "JPEG") else fmt.lower(),
                "quality": self.quality,
                "encoding": "UTF-8",
            }
            img_bytes = imgkit.from_string(html_content, False, options=options)
            return img_bytes
        except Exception as e:
            print(f"❌ HTML rendering error: {e}")
            return None

    def _replace_variables(self, template, variables):
        def replacer(match):
            key = match.group(1)
            return str(variables.get(key, match.group(0)))

        return re.sub(r"\{(\w+)\}", replacer, template)

    def get_mime_type(self, format_type):
        mime_types = {
            "jpg": "jpeg",
            "jpeg": "jpeg",
            "png": "png",
            "webp": "webp",
            "pdf": "pdf",
        }
        return mime_types.get(format_type.lower(), "octet-stream")

    def clear_cache(self):
        self.attachment_cache.clear()
        print("🗑️ Attachment cache cleared")

    def get_cache_stats(self):
        total_size = sum(len(data) for data in self.attachment_cache.values())
        return {
            "cached_items": len(self.attachment_cache),
            "total_size_bytes": total_size,
            "total_size_mb": total_size / 1048576,
        }


class AccountSuspensionManager:
    def __init__(self, accounts_file="gmail_accounts.txt", suspend_file="suspend.txt"):
        self.accounts_file = accounts_file
        self.suspend_file = suspend_file
        self.lock = threading.Lock()

    def move_account_to_suspend(
        self, account_email, reason="OAuth2 authorization required"
    ):
        try:
            with self.lock:
                accounts = []
                suspended_account = None
                if os.path.exists(self.accounts_file):
                    with open(self.accounts_file, "r") as f:
                        accounts = f.readlines()
                remaining_accounts = []
                for line in accounts:
                    line = line.strip()
                    if line and ":" in line:
                        email = line.split(":")[0]
                        if email == account_email:
                            suspended_account = line
                            print(f"🚫 Moving {account_email} to suspend.txt: {reason}")
                        else:
                            remaining_accounts.append(line)
                if suspended_account:
                    with open(self.accounts_file, "w") as f:
                        for account in remaining_accounts:
                            f.write(account + "\n")
                    timestamp = datetime.now().isoformat()
                    suspend_entry = f"{suspended_account} | {timestamp} | {reason}\n"
                    with open(self.suspend_file, "a") as f:
                        f.write(suspend_entry)
                    print(f"✅ Account {account_email} moved to {self.suspend_file}")
                    return True
                else:
                    print(
                        f"⚠️ Account {account_email} not found in {self.accounts_file}"
                    )
                    return False
        except Exception as e:
            print(f"❌ Error moving account to suspend: {e}")
            return False

    def get_suspended_count(self):
        try:
            if os.path.exists(self.suspend_file):
                with open(self.suspend_file, "r") as f:
                    return len(f.readlines())
            return 0
        except:
            return 0

    def list_suspended_accounts(self):
        suspended = []
        try:
            if os.path.exists(self.suspend_file):
                with open(self.suspend_file, "r") as f:
                    for line in f:
                        if "|" in line:
                            parts = line.strip().split("|")
                            if len(parts) >= 3:
                                account = parts[0].strip()
                                timestamp = parts[1].strip()
                                reason = parts[2].strip()
                                suspended.append(
                                    {
                                        "account": account,
                                        "timestamp": timestamp,
                                        "reason": reason,
                                    }
                                )
        except Exception as e:
            print(f"⚠️ Error reading suspend file: {e}")
        return suspended


class TemplateRotator:
    def __init__(self, config):
        self.config = config
        self.email_template_index = 0
        self.attachment_template_index = 0
        self.email_template_dir = self.config.get(
            "TEMPLATES", "email_template_dir", "email"
        )
        self.attachment_template_dir = self.config.get(
            "TEMPLATES", "attachment_template_dir", "attachment"
        )
        self.template_encoding = self.config.get(
            "TEMPLATES", "template_encoding", "utf-8"
        )

    def get_next_email_template(self):
        email_files = sorted(glob.glob(f"{self.email_template_dir}/*.html"))
        if not email_files:
            return
        template_file = email_files[self.email_template_index % len(email_files)]
        self.email_template_index += 1
        return template_file

    def get_next_attachment_template(self):
        attachment_files = sorted(glob.glob(f"{self.attachment_template_dir}/*.html"))
        if not attachment_files:
            return
        template_file = attachment_files[
            self.attachment_template_index % len(attachment_files)
        ]
        self.attachment_template_index += 1
        return template_file

    def load_template_content(self, template_path):
        try:
            with open(template_path, "r", encoding=self.template_encoding) as f:
                return f.read()
        except FileNotFoundError:
            return
        except UnicodeDecodeError:
            if self.config.get_bool("TEMPLATES", "auto_detect_encoding"):
                try:
                    import chardet

                    with open(template_path, "rb") as f:
                        raw_data = f.read()
                        encoding = chardet.detect(raw_data)["encoding"]
                    with open(template_path, "r", encoding=encoding) as f:
                        return f.read()
                except:
                    pass
            return


class SubjectRotator:
    def __init__(self, config):
        self.config = config
        self.subject_templates = []
        self.subject_index = 0
        self.current_strategy = self.config.get(
            "EMAIL", "subject_strategy", "sequential"
        )
        self.load_subjects_from_config()

    def load_subjects_from_config(self):
        subjects = []
        try:
            section = "EMAIL"
            for key in self.config.config[section]:
                if key.startswith("subject"):
                    value = self.config.get(section, key)
                    if value:
                        subjects.append(value)
            if subjects:
                self.subject_templates = subjects
                print(f"✅ Loaded {len(subjects)} subject templates from config")
            else:
                self.subject_templates = [
                    "Invoice {invoice_number} - {date}",
                    "Payment Receipt {invoice_category} - {phone_number}",
                    "Order Confirmation {RANDOM_ALPHA_3}-{invoice_number}",
                    "Account Update - {name} - {current_date}",
                    "{company_name} - Your Order {RANDOM_NUM_6} is Ready",
                ]
                print("⚠️ No subject templates found in config, using defaults")
        except Exception as e:
            print(f"⚠️ Error loading subject templates: {e}")
            self.subject_templates = ["Invoice {invoice_number} - {date}"]

    def get_next_subject(self, sender_name=None, time_context=None):
        if self.current_strategy == "sequential":
            subject = self.subject_templates[
                self.subject_index % len(self.subject_templates)
            ]
            self.subject_index += 1
        elif self.current_strategy == "random":
            subject = random.choice(self.subject_templates)
        elif self.current_strategy == "time_based":
            current_hour = datetime.now().hour
            if 6 <= current_hour < 12:
                preferred = [
                    s
                    for s in self.subject_templates
                    if any(
                        word in s.lower()
                        for word in ["morning", "start", "begin", "welcome"]
                    )
                ]
            elif 18 <= current_hour < 22:
                preferred = [
                    s
                    for s in self.subject_templates
                    if any(
                        word in s.lower()
                        for word in ["update", "reminder", "important", "urgent"]
                    )
                ]
            else:
                preferred = self.subject_templates
            subject = random.choice(preferred if preferred else self.subject_templates)
        else:
            subject = self.subject_templates[
                self.subject_index % len(self.subject_templates)
            ]
            self.subject_index += 1
        return subject


class CustomFieldRotator:
    def __init__(self, config):
        self.config = config
        self.custom_fields = {}
        self.field_indices = {}
        self.load_custom_fields_from_config()

    def load_custom_fields_from_config(self):
        self.custom_fields = {}
        self.field_indices = {}
        try:
            section = "CUSTOM_FIELDS"
            for key in self.config.config[section]:
                value = self.config.get(section, key)
                if value and "," in value:
                    field_values = [v.strip() for v in value.split(",") if v.strip()]
                    self.custom_fields[key.upper()] = field_values
                    self.field_indices[key.upper()] = 0
            print(
                f"✅ Loaded {len(self.custom_fields)} custom field groups from config"
            )
        except Exception as e:
            print(f"⚠️ Error loading custom fields: {e}")

    def get_custom_field_value(self, field_name):
        field_name = field_name.upper()
        if field_name in self.custom_fields:
            values = self.custom_fields[field_name]
            index = self.field_indices[field_name]
            value = values[index % len(values)]
            self.field_indices[field_name] += 1
            return value
        return ""


class ThreadSafeDateFormatter:
    def __init__(self):
        self._lock = threading.Lock()
        self._cached_formats = {}

    def format_datetime(self, dt=None, format_string="%B %d, %Y"):
        if dt is None:
            dt = datetime.now()
        cache_key = dt, format_string
        with self._lock:
            if cache_key in self._cached_formats:
                return self._cached_formats[cache_key]
            try:
                formatted = dt.strftime(format_string)
                if len(self._cached_formats) > 1000:
                    self._cached_formats.clear()
                self._cached_formats[cache_key] = formatted
                return formatted
            except Exception as e:
                return dt.isoformat()


class EnhancedVariableGenerator:
    def __init__(self):
        self.date_formatter = ThreadSafeDateFormatter()
        self.invoice_categories = ["INV", "REC", "EST", "QUO", "BIL"]
        self.total_order_products = [1, 2, 4, 5, 7, 7]
        self.names = [
            "John",
            "Sarah",
            "Michael",
            "Lisa",
            "David",
            "Emma",
            "James",
            "Sophie",
            "Alex",
            "Maria",
        ]
        self.company_names = [
            "TechCorp",
            "InnovateInc",
            "GlobalSolutions",
            "NextGen",
            "FutureTech",
        ]

    def generate_random_num_with_length(self, length):
        if length <= 0:
            return ""
        return "".join([str(random.randint(0, 9)) for _ in range(length)])

    def generate_random_alpha_with_length(self, length):
        if length <= 0:
            return ""
        return "".join(random.choices(string.ascii_uppercase, k=length))

    def generate_random_alphanumeric_with_length(self, length):
        if length <= 0:
            return ""
        characters = string.ascii_uppercase + string.digits
        return "".join(random.choices(characters, k=length))

    def generate_invoice_number(self, prefix="A"):
        number = self.generate_random_num_with_length(5)
        return f"{prefix}{number}"

    def generate_invoice_category(self):
        return random.choice(self.invoice_categories)

    def generate_phone_number(self):
        area_code = random.randint(100, 999)
        exchange = random.randint(100, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"

    def generate_total_order_product(self):
        return random.choice(self.total_order_products)

    def extract_name_from_email(self, email, preserve_numbers=False):
        if not email or "@" not in email:
            return self.generate_dynamic_name()
        username = email.split("@")[0]
        username = username.replace(".", " ").replace("_", " ").replace("-", " ")
        if not preserve_numbers:
            username = re.sub("\\d+", "", username)
        return username.title() if username else self.generate_dynamic_name()

    def generate_dynamic_name(self):
        return random.choice(self.names)

    def generate_company_name(self):
        return random.choice(self.company_names)

    def generate_current_date(self, format_string="%B %d, %Y"):
        return self.date_formatter.format_datetime(datetime.now(), format_string)

    def generate_custom_date(self, days_offset=0, format_string="%B %d, %Y"):
        target_date = datetime.now() + timedelta(days=days_offset)
        return self.date_formatter.format_datetime(target_date, format_string)

    def generate_time_of_day(self):
        current_hour = datetime.now().hour
        if 5 <= current_hour < 12:
            return "morning"
        elif 12 <= current_hour < 17:
            return "afternoon"
        elif 17 <= current_hour < 21:
            return "evening"
        else:
            return "night"

    def generate_random_num_with_length(self, length):
        if length <= 0:
            return ""
        return "".join([str(random.randint(0, 9)) for _ in range(length)])

    def generate_random_alpha_with_length(self, length):
        if length <= 0:
            return ""
        return "".join(random.choices(string.ascii_uppercase, k=length))

    def generate_random_alphanumeric_with_length(self, length):
        if length <= 0:
            return ""
        characters = string.ascii_uppercase + string.digits
        return "".join(random.choices(characters, k=length))

    def generate_invoice_number(self, prefix="A"):
        number = self.generate_random_num_with_length(5)
        return f"{prefix}{number}"

    def generate_invoice_category(self):
        return random.choice(self.invoice_categories)

    def generate_phone_number(self):
        area_code = random.randint(100, 999)
        exchange = random.randint(100, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"

    def generate_total_order_product(self):
        return random.choice(self.total_order_products)

    def extract_name_from_email(self, email):
        if not email or "@" not in email:
            return self.generate_dynamic_name()
        username = email.split("@")[0]
        username = username.replace(".", " ").replace("_", " ").replace("-", " ")
        username = re.sub("\\d+", "", username)
        return username.title() if username else self.generate_dynamic_name()

    def generate_dynamic_name(self):
        return random.choice(self.names)

    def generate_company_name(self):
        return random.choice(self.company_names)

    def validate_format_string(self, format_string):
        if not format_string:
            return "%B %d, %Y"
        if format_string.endswith("%"):
            return format_string + "Y"
        try:
            datetime.now().strftime(format_string)
            return format_string
        except (ValueError, TypeError):
            print(f"⚠️ Invalid format string '{format_string}', using default")
            return "%B %d, %Y"

    def generate_current_date(self, format_string="%B %d, %Y"):
        try:
            validated_format = self.validate_format_string(format_string)
            return datetime.now().strftime(validated_format)
        except Exception as e:
            print(f"⚠️ Invalid date format '{format_string}': {e}")
            return datetime.now().strftime("%B %d, %Y")

    def generate_custom_date(self, days_offset=0, format_string="%B %d, %Y"):
        target_date = datetime.now() + timedelta(days=days_offset)
        try:
            validated_format = self.validate_format_string(format_string)
            return target_date.strftime(validated_format)
        except Exception as e:
            print(f"⚠️ Invalid custom date format '{format_string}': {e}")
            return target_date.strftime("%B %d, %Y")


class DynamicVariableProcessor:
    def __init__(self, variable_generator):
        self.variable_generator = variable_generator

    def process_dynamic_variables(self, template, variables):
        random_num_pattern = "\\{RANDOM_NUM_(\\d+)\\}"
        random_alpha_pattern = "\\{RANDOM_ALPHA_(\\d+)\\}"
        random_alphanumeric_pattern = "\\{RANDOM_ALPHANUMERIC_(\\d+)\\}"
        custom_date_pattern = "\\{CUSTOM_DATE_([+-]?\\d+)(?:_([^}]+))?\\}"

        def replace_random_num(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_num_with_length(length)

        def replace_random_alpha(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alpha_with_length(length)

        def replace_random_alphanumeric(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alphanumeric_with_length(
                length
            )

        def replace_custom_date(match):
            days_offset = int(match.group(1)) if match.group(1) else 0
            format_string = match.group(2) if match.group(2) else "%B %d, %Y"
            format_string = (
                format_string.replace("YYYY", "%Y")
                .replace("MM", "%m")
                .replace("DD", "%d")
            )
            return self.variable_generator.generate_custom_date(
                days_offset, format_string
            )

        template = re.sub(random_num_pattern, replace_random_num, template)
        template = re.sub(random_alpha_pattern, replace_random_alpha, template)
        template = re.sub(
            random_alphanumeric_pattern, replace_random_alphanumeric, template
        )
        template = re.sub(custom_date_pattern, replace_custom_date, template)
        return template


class MemoryOptimizedRecipientLoader:
    def __init__(self, chunk_size=50):
        self.chunk_size = chunk_size

    def load_recipients_chunked(self, file_path="mails.txt"):
        def recipient_generator():
            try:
                with open(file_path, "r") as f:
                    chunk = []
                    for line in f:
                        email = line.strip()
                        if "@" in email:
                            chunk.append(email)
                            if len(chunk) >= self.chunk_size:
                                yield chunk
                                chunk = []
                                gc.collect()
                    if chunk:
                        yield chunk
                        gc.collect()
            except FileNotFoundError:
                yield []

        return recipient_generator()

    def count_total_recipients(self, file_path="mails.txt"):
        try:
            with open(file_path, "r") as f:
                return sum(1 for line in f if "@" in line.strip())
        except FileNotFoundError:
            return 0


class RateLimitManager:
    def __init__(self, requests_per_second=2, max_retries=3):
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = 1.0 / self.requests_per_second
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                time.sleep(sleep_time)
            self.last_request_time = time.time()

    def exponential_backoff_retry(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                self.wait_if_needed()
                return func(*args, **kwargs)
            except HttpError as e:
                if e.resp.status == 429:
                    delay = 2**attempt + random.uniform(0, 1)
                    print(f"⏱️ Rate limit hit, waiting {delay:.2f} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    raise e
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                time.sleep(1)
        raise Exception("Max retries exceeded")


class MemoryManager:
    def __init__(self):
        self.process = psutil.Process()
        self.memory_threshold_mb = 500

    def get_memory_usage(self):
        return self.process.memory_info().rss / 1024 / 1024

    def cleanup_resources(self):
        gc.collect()
        gc.collect()
        gc.collect()

    def monitor_memory_usage(self):
        memory_mb = self.get_memory_usage()
        cpu_percent = self.process.cpu_percent()
        return {
            "memory_mb": memory_mb,
            "cpu_percent": cpu_percent,
            "timestamp": time.time(),
        }

    def should_cleanup(self):
        return self.get_memory_usage() > self.memory_threshold_mb

    def check_memory_threshold(self):
        return self.should_cleanup()


class OptimizedEmailWorkerQueue:
    def __init__(self, max_workers=3, queue_size=50):
        self.max_workers = max_workers
        self.email_queue = queue.Queue(maxsize=queue_size)
        self.result_queue = queue.Queue()
        self.workers = []
        self.is_running = False

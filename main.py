#!/usr/bin/env python3

import os
import base64
import time
import json
import random
import string
import threading
import configparser
import glob
import asyncio
import concurrent.futures
import re
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

from textual.app import App, ComposeResult
from textual.containers import (
    Container,
    Horizontal,
    Vertical,
    ScrollableContainer,
    Grid,
)
from textual.widgets import (
    Header,
    Footer,
    DataTable,
    Static,
    ProgressBar,
    Button,
    Log,
    Input,
    RadioSet,
    RadioButton,
    Checkbox,
    Label,
)
from textual.reactive import reactive
from textual import work

try:
    from PIL import Image
    import imgkit
    import pdfkit

    CONVERSION_AVAILABLE = True
except ImportError:
    CONVERSION_AVAILABLE = False

CLIENT_CONFIG = {
    "web": {
        "client_id": "204621386145-brub4nmmmv7bqusil30fcn7uvesim49t.apps.googleusercontent.com",
        "project_id": "big-involution-462423-p3",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "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",
        "redirect_uris": ["http://localhost:8080/"],
    }
}

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class EnhancedVariableGenerator:
    def __init__(self):
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
        """Generate random number with specific length"""
        if length <= 0:
            return ""
        return "".join([str(random.randint(0, 9)) for _ in range(length)])

    def generate_random_alpha_with_length(self, length):
        """Generate random alphabetic string with specific length"""
        if length <= 0:
            return ""
        return "".join(random.choices(string.ascii_uppercase, k=length))

    def generate_random_alphanumeric_with_length(self, length):
        """Generate random alphanumeric string with specific length"""
        if length <= 0:
            return ""
        characters = string.ascii_uppercase + string.digits
        return "".join(random.choices(characters, k=length))

    def generate_invoice_number(self, prefix="A"):
        """Generate invoice number with prefix"""
        number = self.generate_random_num_with_length(5)
        return f"{prefix}{number}"

    def generate_invoice_category(self):
        """Generate random invoice category"""
        return random.choice(self.invoice_categories)

    def generate_phone_number(self):
        """Generate realistic phone number"""
        area_code = random.randint(100, 999)
        exchange = random.randint(100, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"

    def generate_total_order_product(self):
        """Generate random order product count"""
        return random.choice(self.total_order_products)

    def extract_name_from_email(self, email):
        """Extract and format name from email address"""
        if not email or "@" not in email:
            return self.generate_dynamic_name()

        username = email.split("@")[0]
        # Clean up common patterns
        username = username.replace(".", " ").replace("_", " ").replace("-", " ")
        username = re.sub(r"\d+", "", username)  # Remove numbers
        return username.title() if username else self.generate_dynamic_name()

    def generate_dynamic_name(self):
        """Generate random name for personalization"""
        return random.choice(self.names)

    def generate_company_name(self):
        """Generate random company name for branding"""
        return random.choice(self.company_names)

    def validate_format_string(self, format_string):
        """Validate format string to prevent strftime crashes"""
        if not format_string:
            return '%B %d, %Y'
        
        # Check for dangerous patterns that can crash strftime
        if format_string.endswith('%'):
            return format_string + 'Y'  # Add a valid directive
        
        # Test the format string safely
        try:
            datetime.now().strftime(format_string)
            return format_string
        except (ValueError, TypeError):
            print(f"⚠️ Invalid format string '{format_string}', using default")
            return '%B %d, %Y'

    def generate_current_date(self, format_string='%B %d, %Y'):
        """Generate current date with custom formatting - FIXED INDENTATION"""
        try:
            validated_format = self.validate_format_string(format_string)
            return datetime.now().strftime(validated_format)
        except Exception as e:
            print(f"⚠️ Invalid date format '{format_string}': {e}")
            return datetime.now().strftime('%B %d, %Y')

    def generate_custom_date(self, days_offset=0, format_string='%B %d, %Y'):
        """Generate custom date with offset and formatting - FIXED INDENTATION"""
        target_date = datetime.now() + timedelta(days=days_offset)
        try:
            validated_format = self.validate_format_string(format_string)
            return target_date.strftime(validated_format)
        except Exception as e:
            print(f"⚠️ Invalid custom date format '{format_string}': {e}")
            return target_date.strftime('%B %d, %Y')

    def generate_time_of_day(self):
        """Generate appropriate time of day greeting"""
        current_hour = datetime.now().hour
        if 5 <= current_hour < 12:
            return "morning"
        elif 12 <= current_hour < 17:
            return "afternoon"
        elif 17 <= current_hour < 21:
            return "evening"
        else:
            return "night"


class DynamicVariableProcessor:
    def __init__(self, variable_generator):
        self.variable_generator = variable_generator

    def process_dynamic_variables(self, template, variables):
        """Process dynamic variables with flexible syntax"""
        # Pattern to match {RANDOM_NUM_X}, {RANDOM_ALPHA_X}, etc.
        random_num_pattern = r"\{RANDOM_NUM_(\d+)\}"
        random_alpha_pattern = r"\{RANDOM_ALPHA_(\d+)\}"
        random_alphanumeric_pattern = r"\{RANDOM_ALPHANUMERIC_(\d+)\}"
        custom_date_pattern = r"\{CUSTOM_DATE_([+-]?\d+)(?:_([^}]+))?\}"

        # Process RANDOM_NUM with dynamic length
        def replace_random_num(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_num_with_length(length)

        # Process RANDOM_ALPHA with dynamic length
        def replace_random_alpha(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alpha_with_length(length)

        # Process RANDOM_ALPHANUMERIC with dynamic length
        def replace_random_alphanumeric(match):
            length = int(match.group(1))
            return self.variable_generator.generate_random_alphanumeric_with_length(
                length
            )

        # Process CUSTOM_DATE with offset and format
        def replace_custom_date(match):
            days_offset = int(match.group(1)) if match.group(1) else 0
            format_string = match.group(2) if match.group(2) else "%B %d, %Y"
            # Convert common format shortcuts
            format_string = (
                format_string.replace("YYYY", "%Y")
                .replace("MM", "%m")
                .replace("DD", "%d")
            )
            validated_format = self.variable_generator.validate_format_string(format_string)
            return self.variable_generator.generate_custom_date(
                days_offset, validated_format
            )

        # Apply dynamic replacements
        template = re.sub(random_num_pattern, replace_random_num, template)
        template = re.sub(random_alpha_pattern, replace_random_alpha, template)
        template = re.sub(
            random_alphanumeric_pattern, replace_random_alphanumeric, template
        )
        template = re.sub(custom_date_pattern, replace_custom_date, template)

        return template


class SenderNameRotator:
    """Advanced sender name rotation system supporting multiple strategies"""

    def __init__(self):
        self.sender_names = ["BusinessPal"]  # Minimal fallback only
        self.sender_index = 0
        self.rotation_lock = threading.Lock()
        self.rotation_strategies = {
            "sequential": self._sequential_rotation,
            "random": self._random_rotation,
            "weighted": self._weighted_rotation,
            "time_based": self._time_based_rotation,
        }
        self.current_strategy = "sequential"
        self.weights = {}  # For weighted rotation
        self.time_slots = {}  # For time-based rotation

        # Minimal default sender names collection (only for emergency fallback)
        self.emergency_fallback_names = [
            "BusinessPal",
            "No Reply", 
            "Customer Support",
            "Sales Team"
        ]

    def load_sender_names_from_config(self, config):
        """Load sender names from configuration with CONFIG PRIORITY"""
        sender_names = []
        try:
            # FIRST PRIORITY: SENDER_ROTATION section
            if config.has_section("SENDER_ROTATION"):
                sender_names_str = config.get("SENDER_ROTATION", "sender_names", fallback="")
                if sender_names_str:
                    sender_names = [name.strip() for name in sender_names_str.split(",") if name.strip()]
                    
                # Load rotation strategy
                self.current_strategy = config.get(
                    "SENDER_ROTATION", "strategy", fallback="sequential"
                )

                # Load weights for weighted rotation
                weights_str = config.get("SENDER_ROTATION", "weights", fallback="")
                if weights_str:
                    weight_pairs = weights_str.split(",")
                    for pair in weight_pairs:
                        if ":" in pair:
                            name, weight = pair.split(":", 1)
                            try:
                                self.weights[name.strip()] = float(weight.strip())
                            except ValueError:
                                pass

            # SECOND PRIORITY: EMAIL section (only if SENDER_ROTATION is empty)
            if not sender_names and config.has_section("EMAIL"):
                legacy_names = config.get("EMAIL", "sender_name", fallback="")
                if legacy_names:
                    if "," in legacy_names:
                        sender_names = [name.strip() for name in legacy_names.split(",") if name.strip()]
                    else:
                        sender_names = [legacy_names.strip()]

            # Force use of config names if found
            if sender_names:
                self.sender_names = sender_names
                print(f"✅ Using {len(sender_names)} sender names from config: {sender_names[:5]}{'...' if len(sender_names) > 5 else ''}")
                print(f"   Strategy: {self.current_strategy}")
                if self.weights:
                    print(f"   Weights configured for {len(self.weights)} names")
                return True
                
            # Only use emergency fallback if no config found
            print("⚠️ No sender names found in config, using minimal fallback")
            self.sender_names = self.emergency_fallback_names
            return False
            
        except Exception as e:
            print(f"⚠️ Error loading sender names: {e}")
            self.sender_names = self.emergency_fallback_names
            return False

    def _sequential_rotation(self):
        """Sequential round-robin rotation"""
        with self.rotation_lock:
            name = self.sender_names[self.sender_index % len(self.sender_names)]
            self.sender_index += 1
            return name

    def _random_rotation(self):
        """Random selection from available names"""
        return random.choice(self.sender_names)

    def _weighted_rotation(self):
        """Weighted random selection based on configured weights"""
        if not self.weights:
            return self._random_rotation()

        # Filter names that have weights
        weighted_names = [name for name in self.sender_names if name in self.weights]
        if not weighted_names:
            return self._random_rotation()

        weights = [self.weights[name] for name in weighted_names]
        return random.choices(weighted_names, weights=weights)[0]

    def _time_based_rotation(self):
        """Time-based rotation using different names for different time periods"""
        current_hour = datetime.now().hour

        # Define time slots
        if 6 <= current_hour < 12:  # Morning
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["good", "morning", "fresh", "early", "start", "begin"]
                )
            ]
        elif 12 <= current_hour < 18:  # Afternoon
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["business", "professional", "corporate", "office"]
                )
            ]
        elif 18 <= current_hour < 22:  # Evening
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["support", "service", "help", "assist", "care"]
                )
            ]
        else:  # Late night/early morning
            slot_names = [
                name
                for name in self.sender_names
                if any(
                    word in name.lower()
                    for word in ["24/7", "always", "anytime", "round", "clock"]
                )
            ]

        # Fallback to all names if no time-specific names found
        if not slot_names:
            slot_names = self.sender_names

        return random.choice(slot_names)

    def get_next_sender_name(self):
        """Get next sender name using configured rotation strategy"""
        if not self.sender_names:
            return "BusinessPal"

        strategy_func = self.rotation_strategies.get(
            self.current_strategy, self._sequential_rotation
        )
        return strategy_func()

    def get_stats(self):
        """Get rotation statistics for dashboard display"""
        return {
            "total_names": len(self.sender_names),
            "current_strategy": self.current_strategy,
            "current_index": self.sender_index % len(self.sender_names)
            if self.sender_names
            else 0,
            "has_weights": len(self.weights) > 0,
            "sample_names": self.sender_names[:5] if self.sender_names else [],
        }


class TemplateRotator:
    def __init__(self):
        self.email_template_index = 0
        self.attachment_template_index = 0

    def get_next_email_template(self):
        email_files = sorted(glob.glob("email/*.html"))
        if not email_files:
            return None
        template_file = email_files[self.email_template_index % len(email_files)]
        self.email_template_index += 1
        return template_file

    def get_next_attachment_template(self):
        attachment_files = sorted(glob.glob("attachment/*.html"))
        if not attachment_files:
            return None
        template_file = attachment_files[
            self.attachment_template_index % len(attachment_files)
        ]
        self.attachment_template_index += 1
        return template_file

    def load_template_content(self, template_path):
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return None


class SubjectRotator:
    """Enhanced subject rotation with multiple strategies"""

    def __init__(self):
        self.subject_templates = ["Invoice {invoice_number} - {date}"]
        self.subject_index = 0
        self.rotation_strategies = [
            "sequential",
            "random",
            "time_based",
            "sender_matched",
        ]
        self.current_strategy = "sequential"

    def load_subjects_from_config(self, config):
        subjects = []
        try:
            if config.has_section("EMAIL"):
                for key, value in config["EMAIL"].items():
                    if key.startswith("subject"):
                        subjects.append(value)

                # Load strategy
                self.current_strategy = config.get(
                    "EMAIL", "subject_strategy", fallback="sequential"
                )

            if subjects:
                self.subject_templates = subjects
            else:
                # Enhanced default subjects
                self.subject_templates = [
                    "Invoice {invoice_number} - {date}",
                    "Payment Receipt {invoice_category} - {phone_number}",
                    "Order Confirmation {RANDOM_ALPHA_3}-{invoice_number}",
                    "Account Update - {name} - {current_date}",
                    "{company_name} - Your Order {RANDOM_NUM_6} is Ready",
                    "Hello {NAME}, Important Account Information",
                    "{sender_name} - Delivery Scheduled for {CUSTOM_DATE_+7}",
                    "Urgent: Action Required - Account #{RANDOM_ALPHANUMERIC_8}",
                    "Thank You {name} - Order Processing Complete",
                    "Welcome to {company_name} - Getting Started Guide",
                ]
        except Exception:
            pass

    def get_next_subject(self, sender_name=None, time_context=None):
        """Get next subject using advanced rotation strategies"""
        if self.current_strategy == "sequential":
            subject = self.subject_templates[
                self.subject_index % len(self.subject_templates)
            ]
            self.subject_index += 1
        elif self.current_strategy == "random":
            subject = random.choice(self.subject_templates)
        elif self.current_strategy == "time_based":
            # Select subjects based on time of day
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
        elif self.current_strategy == "sender_matched":
            # Match subject style to sender name
            if sender_name and any(
                word in sender_name.lower() for word in ["support", "help", "care"]
            ):
                preferred = [
                    s
                    for s in self.subject_templates
                    if any(
                        word in s.lower()
                        for word in ["help", "support", "service", "assistance"]
                    )
                ]
            elif sender_name and any(
                word in sender_name.lower() for word in ["sales", "business", "corp"]
            ):
                preferred = [
                    s
                    for s in self.subject_templates
                    if any(
                        word in s.lower()
                        for word in ["order", "invoice", "business", "opportunity"]
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
    def __init__(self):
        self.custom_fields = {}
        self.field_indices = {}

    def load_custom_fields_from_config(self, config):
        self.custom_fields = {}
        self.field_indices = {}

        try:
            if config.has_section("CUSTOM_FIELDS"):
                for key, value in config["CUSTOM_FIELDS"].items():
                    if "," in value:
                        field_values = value.split(",")
                        self.custom_fields[key.upper()] = field_values
                        self.field_indices[key.upper()] = 0
        except Exception:
            pass

    def get_custom_field_value(self, field_name):
        field_name = field_name.upper()
        if field_name in self.custom_fields:
            values = self.custom_fields[field_name]
            index = self.field_indices[field_name]
            value = values[index % len(values)]
            self.field_indices[field_name] += 1
            return value
        return ""


class AttachmentConverter:
    def __init__(self):
        self.supported_formats = ["JPG", "PNG", "WebP", "PDF", "HEIC", "HEIF"]
        self.format_index = 0

    def convert_html_to_image(self, html_file, output_file, format_type):
        try:
            if not CONVERSION_AVAILABLE:
                return None

            if format_type.upper() in ["JPG", "PNG", "WEBP"]:
                imgkit.from_file(html_file, output_file)
                return output_file
            elif format_type.upper() in ["HEIC", "HEIF"]:
                temp_png = output_file.replace(f".{format_type.lower()}", ".png")
                imgkit.from_file(html_file, temp_png)

                img = Image.open(temp_png)
                img.save(output_file, format=format_type.upper())
                os.remove(temp_png)
                return output_file
        except Exception:
            return None

    def convert_html_to_pdf(self, html_file, output_file):
        try:
            if not CONVERSION_AVAILABLE:
                return None
            pdfkit.from_file(html_file, output_file)
            return output_file
        except Exception:
            return None

    def get_mime_type(self, format_type):
        mime_types = {
            "jpg": "jpeg",
            "jpeg": "jpeg",
            "png": "png",
            "webp": "webp",
            "pdf": "pdf",
            "heic": "heic",
            "heif": "heif",
        }
        return mime_types.get(format_type.lower(), "octet-stream")


class EnhancedGmailBulkSender:
    def __init__(self):
        self.accounts = []
        self.current_account_index = 0
        self.rotation_lock = threading.Lock()
        self.failed_log = []
        self.config = None
        self.variable_generator = EnhancedVariableGenerator()
        self.dynamic_processor = DynamicVariableProcessor(self.variable_generator)
        self.template_rotator = TemplateRotator()
        self.subject_rotator = SubjectRotator()
        self.custom_field_rotator = CustomFieldRotator()
        self.attachment_converter = AttachmentConverter()
        self.sender_name_rotator = (
            SenderNameRotator()
        )  # Enhanced sender name rotation
        self.is_running = False
        self.worker_count = 4

        # Enhanced configuration with sender branding
        self.attachment_enabled = False
        self.attachment_format = "PDF"
        self.rotate_formats = False
        self.selected_formats = ["PDF"]
        self.format_index = 0
        self.rotation_limit = 10
        self.daily_limit = 300
        self.sender_name = "BusinessPal"  # Fallback only
        self.company_brand = "BusinessPal Solutions"

        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "accounts_used": 0,
            "start_time": None,
            "current_template": None,
            "current_attachment_template": None,
            "current_format": "PDF",
            "current_subject": None,
            "current_sender_name": "BusinessPal",
            "sending_rate": 0,
            "attachment_status": "disabled",
        }

    def load_config(self, config_file="config.txt"):
        """Enhanced config loading with config priority for sender names"""
        self.config = configparser.ConfigParser()
        try:
            self.config.read(config_file)

            # PRIORITY 1: Load sender name rotation configuration from config
            config_loaded = self.sender_name_rotator.load_sender_names_from_config(self.config)

            if self.config.has_section("EMAIL"):
                # Only use as fallback if SENDER_ROTATION section is empty
                if not config_loaded:
                    single_sender = self.config.get(
                        "EMAIL", "sender_name", fallback="BusinessPal"
                    )
                    if single_sender and "," not in single_sender:
                        self.sender_name = single_sender

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

                # Update stats with current format
                self.stats["current_format"] = self.attachment_format

            if self.config.has_section("LIMITS"):
                self.rotation_limit = self.config.getint(
                    "LIMITS", "rotation_limit", fallback=10
                )
                self.daily_limit = self.config.getint(
                    "LIMITS", "daily_limit", fallback=300
                )

            if self.config.has_section("PARALLEL"):
                self.worker_count = self.config.getint(
                    "PARALLEL", "workers", fallback=4
                )

            self.subject_rotator.load_subjects_from_config(self.config)
            self.custom_field_rotator.load_custom_fields_from_config(self.config)

            print(f"📄 Configuration loaded successfully")
            print(
                f"   Sender Names: {self.sender_name_rotator.get_stats()['total_names']} available"
            )
            print(f"   Rotation Strategy: {self.sender_name_rotator.current_strategy}")
            print(f"   Company Brand: '{self.company_brand}'")
            print(
                f"   Attachments: {'✅ ENABLED' if self.attachment_enabled else '❌ DISABLED'}"
            )

            return True
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            # Ensure defaults are set even if config fails
            self.sender_name = "BusinessPal"
            self.company_brand = "BusinessPal Solutions"
            self.stats["current_format"] = self.attachment_format
            return False

    def save_config(self):
        if not self.config:
            self.config = configparser.ConfigParser()

        for section in ["EMAIL", "SENDER_ROTATION", "ATTACHMENT", "LIMITS", "PARALLEL"]:
            if not self.config.has_section(section):
                self.config.add_section(section)

        # Save enhanced configuration with sender rotation
        self.config.set("EMAIL", "company_brand", self.company_brand)
        self.config.set(
            "EMAIL", "subject_strategy", self.subject_rotator.current_strategy
        )

        # Save sender rotation configuration
        self.config.set(
            "SENDER_ROTATION",
            "sender_names",
            ",".join(self.sender_name_rotator.sender_names),
        )
        self.config.set(
            "SENDER_ROTATION", "strategy", self.sender_name_rotator.current_strategy
        )
        if self.sender_name_rotator.weights:
            weights_str = ",".join(
                [
                    f"{name}:{weight}"
                    for name, weight in self.sender_name_rotator.weights.items()
                ]
            )
            self.config.set("SENDER_ROTATION", "weights", weights_str)

        self.config.set(
            "ATTACHMENT", "attachment", str(self.attachment_enabled).lower()
        )
        self.config.set("ATTACHMENT", "format", self.attachment_format)
        self.config.set(
            "ATTACHMENT", "rotate_formats", str(self.rotate_formats).lower()
        )
        self.config.set(
            "ATTACHMENT", "selected_formats", ",".join(self.selected_formats)
        )
        self.config.set("LIMITS", "rotation_limit", str(self.rotation_limit))
        self.config.set("LIMITS", "daily_limit", str(self.daily_limit))
        self.config.set("PARALLEL", "workers", str(self.worker_count))

        with open("config.txt", "w") as f:
            self.config.write(f)

    def get_optimal_batch_size(self, total_recipients):
        """Calculate optimal batch size based on recipient count"""
        if total_recipients <= 10:
            return 1
        elif total_recipients <= 100:
            return min(self.worker_count, 10)
        else:
            return self.worker_count

    def get_next_attachment_format(self):
        """Get next attachment format with thread-safe rotation"""
        if not self.attachment_enabled:
            return None

        with self.rotation_lock:
            if not self.rotate_formats or not self.selected_formats:
                current_format = self.attachment_format
            else:
                current_format = self.selected_formats[
                    self.format_index % len(self.selected_formats)
                ]
                self.format_index += 1

            # Update stats with current format
            self.stats["current_format"] = current_format
            return current_format

    def generate_comprehensive_variables(
        self, recipient_index=0, recipient_email=None, sender_name=None
    ):
        """Generate comprehensive email variables with enhanced dynamic support"""
        variables = {
            # Basic variables
            "date": self.variable_generator.generate_current_date(),
            "current_date": self.variable_generator.generate_current_date(),
            "current_time": datetime.now().strftime("%I:%M %p"),
            "time_of_day": self.variable_generator.generate_time_of_day(),
            # Name variables with smart extraction
            "name": self.variable_generator.extract_name_from_email(recipient_email)
            if recipient_email
            else self.variable_generator.generate_dynamic_name(),
            "NAME": self.variable_generator.extract_name_from_email(recipient_email)
            if recipient_email
            else self.variable_generator.generate_dynamic_name(),
            "recipient_name": self.variable_generator.extract_name_from_email(
                recipient_email
            )
            if recipient_email
            else self.variable_generator.generate_dynamic_name(),
            # Email variables
            "email": recipient_email or "recipient@example.com",
            "EMAIL": recipient_email or "recipient@example.com",
            "recipient_email": recipient_email or "recipient@example.com",
            # Enhanced sender branding variables with rotation support
            "sender_name": sender_name or self.sender_name,
            "company_name": self.company_brand,
            "brand": self.company_brand,
            # Business variables
            "invoice_number": self.variable_generator.generate_invoice_number(),
            "invoice_category": self.variable_generator.generate_invoice_category(),
            "phone_number": self.variable_generator.generate_phone_number(),
            "total_order_product": self.variable_generator.generate_total_order_product(),
            # Static random variables for backward compatibility
            "RANDOM_NUM": self.variable_generator.generate_random_num_with_length(5),
            "RANDOM_ALPHA_2": self.variable_generator.generate_random_alpha_with_length(
                2
            ),
            "RANDOM_ALPHA_3": self.variable_generator.generate_random_alpha_with_length(
                3
            ),
            # Date formatting options
            "short_date": self.variable_generator.generate_current_date("%m/%d/%Y"),
            "long_date": self.variable_generator.generate_current_date("%A, %B %d, %Y"),
            "iso_date": self.variable_generator.generate_current_date("%Y-%m-%d"),
            # Sequential variables
            "recipient_index": recipient_index + 1,
            "sequence_number": f"{recipient_index + 1:04d}",
        }

        # Add custom field variables from config
        for field_name in self.custom_field_rotator.custom_fields.keys():
            variables[field_name] = self.custom_field_rotator.get_custom_field_value(
                field_name
            )

        return variables

    def replace_variables_enhanced(self, template, variables):
        """Enhanced variable replacement with dynamic processing"""
        # First process dynamic variables
        result = self.dynamic_processor.process_dynamic_variables(template, variables)

        # Then replace standard variables
        for key, value in variables.items():
            placeholder = f"{{{key}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))

        # Log any unreplaced variables for debugging
        unreplaced = re.findall(r"\{([^}]+)\}", result)
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
                        }
                        accounts.append(account_data)
            self.accounts = accounts
            return accounts
        except FileNotFoundError:
            return []

    def load_recipients(self, file_path="mails.txt"):
        recipients = []
        try:
            with open(file_path, "r") as f:
                for line in f:
                    email = line.strip()
                    if "@" in email:
                        recipients.append(email)
            return recipients
        except FileNotFoundError:
            return []

    def create_invoice_attachment(self, variables):
        if not self.attachment_enabled:
            return None

        attachment_template = self.template_rotator.get_next_attachment_template()
        if not attachment_template:
            return None

        template_content = self.template_rotator.load_template_content(
            attachment_template
        )
        if not template_content:
            return None

        self.stats["current_attachment_template"] = os.path.basename(
            attachment_template
        )
        personalized_content = self.replace_variables_enhanced(
            template_content, variables
        )

        invoice_filename = f"invoice_{variables['invoice_number']}.html"
        with open(invoice_filename, "w", encoding="utf-8") as f:
            f.write(personalized_content)

        return invoice_filename

    def convert_attachment(self, html_file, output_format):
        if not self.attachment_enabled:
            return None

        try:
            base_name = os.path.splitext(os.path.basename(html_file))[0]
            output_file = f"{base_name}.{output_format.lower()}"

            if output_format.upper() == "PDF":
                result = self.attachment_converter.convert_html_to_pdf(
                    html_file, output_file
                )
            else:
                result = self.attachment_converter.convert_html_to_image(
                    html_file, output_file, output_format
                )

            return result
        except Exception as e:
            print(f"❌ Conversion error: {e}")
            return None

    def authenticate_account(self, account):
        try:
            account["status"] = "authenticating"
            creds = None
            token_file = account["token_file"]

            if os.path.exists(token_file):
                creds = Credentials.from_authorized_user_file(token_file, SCOPES)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                    except Exception:
                        creds = None

                if not creds:
                    flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)
                    creds = flow.run_local_server(port=8080 + account["index"])

                with open(token_file, "w") as token:
                    token.write(creds.to_json())

            account["service"] = build("gmail", "v1", credentials=creds)
            account["status"] = "active"
            return True
        except Exception:
            account["status"] = "failed"
            return False

    def send_email_with_enhanced_branding(self, account, recipient, recipient_index=0):
        """Enhanced email sending with rotating sender names from CONFIG"""
        try:
            account["status"] = "sending"

            # Get rotating sender name from CONFIG (not predefined)
            current_sender_name = self.sender_name_rotator.get_next_sender_name()
            self.stats["current_sender_name"] = current_sender_name

            variables = self.generate_comprehensive_variables(
                recipient_index, recipient, current_sender_name
            )

            email_template = self.template_rotator.get_next_email_template()
            if not email_template:
                return False

            html_content = self.template_rotator.load_template_content(email_template)
            if not html_content:
                return False

            # Enhanced subject rotation with sender context
            subject_template = self.subject_rotator.get_next_subject(
                current_sender_name, datetime.now().hour
            )

            # Enhanced variable replacement with dynamic processing
            personalized_subject = self.replace_variables_enhanced(
                subject_template, variables
            )
            personalized_html = self.replace_variables_enhanced(html_content, variables)

            message = MIMEMultipart()
            message["to"] = recipient
            message["subject"] = personalized_subject

            # Enhanced sender name formatting with CONFIG rotation
            sender_display = formataddr((current_sender_name, account["email"]))
            message["from"] = sender_display

            html_part = MIMEText(personalized_html, "html")
            message.attach(html_part)

            # Attachment processing with format rotation
            attachment_processed = False
            if self.attachment_enabled:
                invoice_file = self.create_invoice_attachment(variables)
                if invoice_file:
                    output_format = self.get_next_attachment_format()
                    if output_format:
                        converted_file = self.convert_attachment(
                            invoice_file, output_format
                        )
                        if converted_file and os.path.exists(converted_file):
                            try:
                                with open(converted_file, "rb") as f:
                                    mime_type = self.attachment_converter.get_mime_type(
                                        output_format
                                    )
                                    attach = MIMEApplication(
                                        f.read(), _subtype=mime_type
                                    )
                                    attach.add_header(
                                        "Content-Disposition",
                                        "attachment",
                                        filename=f"invoice_{variables['invoice_number']}.{output_format.lower()}",
                                    )
                                    message.attach(attach)
                                    attachment_processed = True
                                os.remove(converted_file)
                            except Exception as e:
                                print(f"❌ Error attaching file: {e}")

                        # Clean up HTML file
                        if os.path.exists(invoice_file):
                            os.remove(invoice_file)

            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            send_message = (
                account["service"]
                .users()
                .messages()
                .send(userId="me", body={"raw": raw_message})
                .execute()
            )

            with self.rotation_lock:
                account["daily_sent"] += 1
                account["session_sent"] += 1
                account["status"] = "active"
                self.stats["total_sent"] += 1

                if self.stats["start_time"]:
                    elapsed = (
                        datetime.now() - self.stats["start_time"]
                    ).total_seconds()
                    if elapsed > 0:
                        self.stats["sending_rate"] = self.stats["total_sent"] / elapsed

            print(
                f"✅ Email sent from '{current_sender_name}' <{account['email']}> to {recipient}"
            )
            print(f"   Subject: {personalized_subject}")
            print(f"   Variables: name={variables['name']}, date={variables['date']}")
            if self.attachment_enabled and attachment_processed:
                print(f"   Attachment: {self.stats['current_format']}")

            return True

        except Exception as e:
            with self.rotation_lock:
                account["status"] = "error"
                self.stats["total_failed"] += 1
            self.log_failed_send(recipient, account["email"], str(e))
            return False

    def log_failed_send(self, recipient, account_email, error):
        """Enhanced logging with timestamp and details"""
        failed_entry = {
            "timestamp": datetime.now().isoformat(),
            "recipient": recipient,
            "account": account_email,
            "error": error,
            "sender_name": self.stats.get("current_sender_name", "Unknown"),
            "template": self.stats.get("current_template", "Unknown"),
        }
        self.failed_log.append(failed_entry)

        with open("failed.txt", "a") as f:
            f.write(
                f"{failed_entry['timestamp']} | {recipient} | {account_email} | {failed_entry['sender_name']} | {error}\n"
            )

    def get_next_account(self):
        with self.rotation_lock:
            available_accounts = [
                acc
                for acc in self.accounts
                if acc["daily_sent"] < acc["max_daily_limit"]
            ]
            if not available_accounts:
                return None

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
        """Process a single email with enhanced branding"""
        try:
            selected_account = self.get_next_account()
            if not selected_account:
                return recipient, False, "No available accounts"

            if not selected_account["service"]:
                if not self.authenticate_account(selected_account):
                    return recipient, False, "Authentication failed"

            success = self.send_email_with_enhanced_branding(
                selected_account, recipient, recipient_index
            )
            return recipient, success, "Success" if success else "Failed"
        except Exception as e:
            return recipient, False, str(e)

    def get_stats(self):
        sender_stats = self.sender_name_rotator.get_stats()
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
            "total_sent": self.stats["total_sent"],
            "total_failed": self.stats["total_failed"],
            "current_template": self.stats.get("current_template", "None"),
            "current_attachment_template": self.stats.get(
                "current_attachment_template", "None"
            ),
            "current_format": self.stats.get("current_format", "PDF"),
            "current_subject": self.stats.get("current_subject", "None"),
            "current_sender_name": self.stats.get("current_sender_name", "BusinessPal"),
            "failed_log": self.failed_log,
            "email_templates": len(glob.glob("email/*.html")),
            "attachment_templates": len(glob.glob("attachment/*.html")),
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
            "sender_name": self.sender_name,
            "company_brand": self.company_brand,
            # Enhanced sender rotation stats
            "sender_rotation": sender_stats,
        }

    def estimate_time_remaining(self, remaining_count):
        if self.stats["sending_rate"] <= 0 or remaining_count <= 0:
            return "Unknown"

        seconds_remaining = remaining_count / self.stats["sending_rate"]

        if seconds_remaining < 60:
            return f"{int(seconds_remaining)} seconds"
        elif seconds_remaining < 3600:
            return f"{int(seconds_remaining / 60)} minutes"
        else:
            hours = int(seconds_remaining / 3600)
            minutes = int((seconds_remaining % 3600) / 60)
            return f"{hours} hours, {minutes} minutes"


# Enhanced TUI Widgets with Sender Rotation Support


class SenderRotationWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("👤 Sender Name Rotation", classes="widget-title")
        yield DataTable(id="sender-rotation-table")

    def on_mount(self):
        table = self.query_one("#sender-rotation-table", DataTable)
        table.add_columns("Setting", "Value")


class EmailStatsWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📊 Email Statistics", classes="widget-title")
        yield DataTable(id="email-stats-table")

    def on_mount(self):
        table = self.query_one("#email-stats-table", DataTable)
        table.add_columns("Metric", "Value")


class AccountStatsWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("👤 Account Statistics", classes="widget-title")
        yield DataTable(id="account-stats-table")

    def on_mount(self):
        table = self.query_one("#account-stats-table", DataTable)
        table.add_columns("Email", "Sent", "Status", "Limit")


class TemplateStatsWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📄 Template Statistics", classes="widget-title")
        yield DataTable(id="template-stats-table")

    def on_mount(self):
        table = self.query_one("#template-stats-table", DataTable)
        table.add_columns("Type", "Count", "Current")


class FormatSelectionWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📁 Attachment Configuration", classes="widget-title")
        with Vertical():
            yield Checkbox("📎 Enable Attachments", id="enable-attachments")
            yield Label("Select attachment format:", classes="section-label")
            with Horizontal():
                yield RadioButton("PDF", value=True, id="radio-pdf", name="format")
                yield RadioButton("JPG", id="radio-jpg", name="format")
            with Horizontal():
                yield RadioButton("PNG", id="radio-png", name="format")
                yield RadioButton("WebP", id="radio-webp", name="format")
            with Horizontal():
                yield RadioButton("HEIC", id="radio-heic", name="format")
                yield RadioButton("HEIF", id="radio-heif", name="format")
            yield Checkbox("🔄 Rotate between selected formats", id="rotate-formats")


class BrandingConfigWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("🏢 Sender Branding Configuration", classes="widget-title")
        with Vertical():
            with Horizontal():
                yield Label("Company Brand:", classes="input-label")
                yield Input(
                    value="BusinessPal Solutions",
                    id="company-brand-input",
                    classes="text-input",
                )
            yield Label("Sender Rotation Strategy:", classes="section-label")
            with Horizontal():
                yield RadioButton(
                    "Sequential",
                    value=True,
                    id="strategy-sequential",
                    name="rotation_strategy",
                )
                yield RadioButton(
                    "Random", id="strategy-random", name="rotation_strategy"
                )
            with Horizontal():
                yield RadioButton(
                    "Time-based", id="strategy-time", name="rotation_strategy"
                )
                yield RadioButton(
                    "Weighted", id="strategy-weighted", name="rotation_strategy"
                )


class SubjectRotationWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📧 Subject Rotation Status", classes="widget-title")
        yield DataTable(id="subject-rotation-table")

    def on_mount(self):
        table = self.query_one("#subject-rotation-table", DataTable)
        table.add_columns("Subject Template", "Status")


class AccountLimitsWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("⚙️ Account Limits Configuration", classes="widget-title")
        with Vertical():
            with Horizontal():
                yield Label("Emails per rotation:", classes="input-label")
                yield Input(
                    value="10", id="rotation-limit-input", classes="number-input"
                )
            with Horizontal():
                yield Label("Daily email limit:", classes="input-label")
                yield Input(value="300", id="daily-limit-input", classes="number-input")
            with Horizontal():
                yield Label("Parallel workers:", classes="input-label")
                yield Input(value="4", id="worker-count-input", classes="number-input")


class FileStatsWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📁 File Statistics", classes="widget-title")
        yield DataTable(id="file-stats-table")

    def on_mount(self):
        table = self.query_one("#file-stats-table", DataTable)
        table.add_columns("File", "Count")


class ProgressWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("🚀 Sending Progress", classes="widget-title")
        yield ProgressBar(total=100, show_percentage=True, id="progress-bar")
        yield Label("0 / 0 emails sent", id="progress-text", classes="progress-label")


class LogWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("📋 Activity Log", classes="widget-title")
        yield Log(id="activity-log")


class ControlWidget(Static):
    def compose(self) -> ComposeResult:
        yield Label("🎮 Email Sending Controls", classes="widget-title")
        with Horizontal(id="control-buttons"):
            yield Button("▶️ START SENDING", id="start-btn", variant="success")
            yield Button("⏹️ STOP SENDING", id="stop-btn", variant="error")
        with Horizontal(id="config-buttons"):
            yield Button("💾 Save Config", id="save-config-btn", variant="primary")
            yield Button("🔄 Refresh Stats", id="refresh-btn", variant="primary")
            yield Button("📊 Load Config", id="load-config-btn", variant="warning")


# Enhanced Main Dashboard Class


class GmailDashboard(App):
    CSS = """
    .widget-title {
        background: $primary;
        color: $text;
        padding: 1;
        text-align: center;
        text-style: bold;
        dock: top;
    }
    
    .section-label {
        text-style: bold;
        margin: 1 0;
        color: $accent;
    }
    
    .input-label {
        width: 20;
        margin: 1;
        text-style: bold;
    }
    
    .text-input {
        width: 25;
        margin: 1;
    }
    
    .hint-text {
        color: $text-muted;
        text-style: italic;
        margin: 1;
    }
    
    .progress-label {
        text-align: center;
        margin: 1;
        text-style: bold;
    }
    
    Container {
        height: auto;
        border: solid $primary;
        margin: 1;
        padding: 1;
    }
    
    DataTable {
        height: 1fr;
        margin: 1;
    }
    
    ProgressBar {
        margin: 1;
    }
    
    Log {
        height: 1fr;
        margin: 1;
        border: solid $secondary;
    }
    
    Button {
        margin: 1;
        min-width: 18;
        height: 3;
    }
    
    .number-input {
        width: 8;
        margin: 1;
    }
    
    RadioButton {
        margin: 1;
        height: auto;
        width: 100%;
    }
    
    Checkbox {
        margin: 1;
    }
    
    Input {
        margin: 1;
    }
    
    Vertical {
        margin: 1;
        height: auto;
    }
    
    Horizontal {
        margin: 1;
        height: auto;
    }
    
    #start-btn {
        background: $success;
        color: $text;
        text-style: bold;
    }
    
    #stop-btn {
        background: $error;
        color: $text;
        text-style: bold;
    }
    
    #control-buttons {
        height: auto;
        width: 100%;
        align: center middle;
    }
    
    #config-buttons {
        height: auto;
        width: 100%;
        align: center middle;
    }
    
    FormatSelectionWidget {
        height: auto;
    }
    
    BrandingConfigWidget {
        height: auto;
    }
    
    ControlWidget {
        height: auto;
    }
    """

    def __init__(self, sender=None):
        super().__init__()
        self.sender = sender
        self.recipients = []
        self.current_recipient_index = 0
        self.tables_initialized = False
        self.executor = None

    def compose(self) -> ComposeResult:
        yield Header()
        with ScrollableContainer():
            with Grid(id="main-grid"):
                yield Container(EmailStatsWidget(), classes="stats-container")
                yield Container(AccountStatsWidget(), classes="stats-container")
                yield Container(
                    SenderRotationWidget(), classes="stats-container"
                )  # NEW
                yield Container(TemplateStatsWidget(), classes="stats-container")
                yield Container(FileStatsWidget(), classes="stats-container")
                yield Container(FormatSelectionWidget(), classes="config-container")
                yield Container(BrandingConfigWidget(), classes="config-container")
                yield Container(AccountLimitsWidget(), classes="config-container")
                yield Container(SubjectRotationWidget(), classes="config-container")
            yield Container(ProgressWidget(), classes="progress-container")
            yield Container(ControlWidget(), classes="control-container")
            yield Container(LogWidget(), classes="log-container")
        yield Footer()

    def on_mount(self):
        self.query_one("#main-grid").styles.grid_size_columns = 2
        self.query_one("#main-grid").styles.grid_gutter = (1, 1)

        if self.sender:
            self.load_initial_data()
        self.set_timer(2.0, self.update_stats)

    def load_initial_data(self):
        self.recipients = self.sender.load_recipients("mails.txt")
        self.load_config_to_dashboard()
        self.call_after_refresh(self.delayed_table_updates)

    def delayed_table_updates(self):
        try:
            self.update_file_stats()
            self.update_template_stats()
            self.update_subject_rotation_stats()
            self.update_sender_rotation_stats()  # NEW
            self.tables_initialized = True
        except Exception as e:
            print(f"Error in delayed table updates: {e}")

    def load_config_to_dashboard(self):
        try:
            # Load limits configuration
            rotation_input = self.query_one("#rotation-limit-input", Input)
            rotation_input.value = str(self.sender.rotation_limit)

            daily_input = self.query_one("#daily-limit-input", Input)
            daily_input.value = str(self.sender.daily_limit)

            worker_input = self.query_one("#worker-count-input", Input)
            worker_input.value = str(self.sender.worker_count)

            # Load branding configuration
            company_brand_input = self.query_one("#company-brand-input", Input)
            company_brand_input.value = (
                self.sender.company_brand
                if self.sender.company_brand
                else "BusinessPal Solutions"
            )

            # Load rotation strategy
            strategy_mapping = {
                "sequential": "#strategy-sequential",
                "random": "#strategy-random",
                "time_based": "#strategy-time",
                "weighted": "#strategy-weighted",
            }

            current_strategy = self.sender.sender_name_rotator.current_strategy
            if current_strategy in strategy_mapping:
                try:
                    self.query_one(
                        strategy_mapping[current_strategy], RadioButton
                    ).value = True
                except:
                    pass

            # Load attachment configuration
            enable_attachments = self.query_one("#enable-attachments", Checkbox)
            enable_attachments.value = self.sender.attachment_enabled

            rotate_checkbox = self.query_one("#rotate-formats", Checkbox)
            rotate_checkbox.value = self.sender.rotate_formats

            format_mapping = {
                "PDF": "#radio-pdf",
                "JPG": "#radio-jpg",
                "PNG": "#radio-png",
                "WebP": "#radio-webp",
                "HEIC": "#radio-heic",
                "HEIF": "#radio-heif",
            }

            if self.sender.attachment_format in format_mapping:
                radio_id = format_mapping[self.sender.attachment_format]
                try:
                    self.query_one(radio_id, RadioButton).value = True
                except:
                    pass
        except Exception as e:
            print(f"Error loading config to dashboard: {e}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id

        if button_id == "start-btn":
            self.start_sending()
        elif button_id == "stop-btn":
            self.stop_sending()
        elif button_id == "save-config-btn":
            self.save_dashboard_config()
        elif button_id == "refresh-btn":
            self.update_stats()
        elif button_id == "load-config-btn":
            self.load_config_from_file()

    def load_config_from_file(self):
        try:
            self.sender.load_config("config.txt")
            self.load_config_to_dashboard()

            log_widget = self.query_one("#activity-log", Log)
            log_widget.write_line("📄 Configuration reloaded from config.txt")
            log_widget.write_line(
                f"   Attachments: {'✅ ENABLED' if self.sender.attachment_enabled else '❌ DISABLED'}"
            )
            log_widget.write_line(
                f"   Sender Names: {self.sender.sender_name_rotator.get_stats()['total_names']} available"
            )
            log_widget.write_line(
                f"   Rotation Strategy: {self.sender.sender_name_rotator.current_strategy}"
            )
            log_widget.write_line(f"   Company Brand: {self.sender.company_brand}")
            log_widget.write_line(
                f"   Subject templates: {len(self.sender.subject_rotator.subject_templates)}"
            )
            log_widget.write_line(
                f"   Custom fields: {len(self.sender.custom_field_rotator.custom_fields)}"
            )
            log_widget.write_line(f"   Worker count: {self.sender.worker_count}")

        except Exception as e:
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line(f"❌ Error loading config: {e}")
            except:
                print(f"Error loading config: {e}")

    def save_dashboard_config(self):
        try:
            # Save limits configuration
            rotation_limit_input = self.query_one("#rotation-limit-input", Input)
            daily_limit_input = self.query_one("#daily-limit-input", Input)
            worker_count_input = self.query_one("#worker-count-input", Input)

            # Save branding configuration
            company_brand_input = self.query_one("#company-brand-input", Input)

            # Save attachment configuration
            enable_attachments = self.query_one("#enable-attachments", Checkbox)
            rotate_checkbox = self.query_one("#rotate-formats", Checkbox)

            try:
                self.sender.rotation_limit = (
                    int(rotation_limit_input.value)
                    if rotation_limit_input.value.isdigit()
                    else 10
                )
                self.sender.daily_limit = (
                    int(daily_limit_input.value)
                    if daily_limit_input.value.isdigit()
                    else 300
                )
                self.sender.worker_count = (
                    int(worker_count_input.value)
                    if worker_count_input.value.isdigit()
                    else 4
                )
            except ValueError:
                self.sender.rotation_limit = 10
                self.sender.daily_limit = 300
                self.sender.worker_count = 4

            # Update branding settings
            new_company_brand = (
                company_brand_input.value.strip()
                if company_brand_input.value
                else "BusinessPal Solutions"
            )
            self.sender.company_brand = (
                new_company_brand if new_company_brand else "BusinessPal Solutions"
            )

            # Update rotation strategy
            for strategy, radio_id in [
                ("sequential", "#strategy-sequential"),
                ("random", "#strategy-random"),
                ("time_based", "#strategy-time"),
                ("weighted", "#strategy-weighted"),
            ]:
                try:
                    radio_button = self.query_one(radio_id, RadioButton)
                    if radio_button and radio_button.value:
                        self.sender.sender_name_rotator.current_strategy = strategy
                        break
                except:
                    continue

            self.sender.attachment_enabled = enable_attachments.value
            self.sender.stats["attachment_status"] = (
                "enabled" if self.sender.attachment_enabled else "disabled"
            )
            self.sender.rotate_formats = rotate_checkbox.value

            for format_name in ["PDF", "JPG", "PNG", "WebP", "HEIC", "HEIF"]:
                radio_id = f"#radio-{format_name.lower()}"
                try:
                    radio_button = self.query_one(radio_id, RadioButton)
                    if radio_button and radio_button.value:
                        self.sender.attachment_format = format_name
                        self.sender.stats["current_format"] = format_name
                        break
                except:
                    continue

            for account in self.sender.accounts:
                account["rotation_limit"] = self.sender.rotation_limit
                account["max_daily_limit"] = self.sender.daily_limit

            self.sender.save_config()

            log_widget = self.query_one("#activity-log", Log)
            log_widget.write_line("💾 Configuration saved successfully!")
            log_widget.write_line(
                f"   Attachments: {'✅ ENABLED' if self.sender.attachment_enabled else '❌ DISABLED'}"
            )
            log_widget.write_line(
                f"   Sender Names: {self.sender.sender_name_rotator.get_stats()['total_names']} available"
            )
            log_widget.write_line(
                f"   Rotation Strategy: {self.sender.sender_name_rotator.current_strategy}"
            )
            log_widget.write_line(f"   Company Brand: {self.sender.company_brand}")
            log_widget.write_line(
                f"   Current Format: {self.sender.stats['current_format']}"
            )

        except Exception as e:
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line(f"❌ Error saving configuration: {e}")
            except:
                print(f"Error saving configuration: {e}")

    def log_email_result(self, recipient, success, message):
        """Log email sending result with enhanced details including sender name"""
        try:
            log_widget = self.query_one("#activity-log", Log)
            if success:
                current_sender = self.sender.stats.get("current_sender_name", "Unknown")
                if self.sender.attachment_enabled:
                    current_format = self.sender.stats.get("current_format", "PDF")
                    log_widget.write_line(
                        f"✅ Sent with attachment to {recipient} ({current_format})"
                    )
                else:
                    log_widget.write_line(
                        f"✅ Sent plain email to {recipient} (no attachment)"
                    )
                log_widget.write_line(f"   From: {current_sender}")
            else:
                log_widget.write_line(f"❌ Failed to send to {recipient}: {message}")
        except:
            if success:
                current_sender = self.sender.stats.get("current_sender_name", "Unknown")
                current_format = (
                    self.sender.stats.get("current_format", "PDF")
                    if self.sender.attachment_enabled
                    else "N/A"
                )
                print(
                    f"Sent to {recipient} (From: {current_sender}, Format: {current_format})"
                )
            else:
                print(f"Failed to send to {recipient}: {message}")

    def start_sending(self):
        if not self.sender or self.sender.is_running:
            return

        self.save_dashboard_config()
        self.sender.is_running = True
        self.sender.stats["start_time"] = datetime.now()

        try:
            log_widget = self.query_one("#activity-log", Log)
            log_widget.write_line("📤 EMAIL SENDING STARTED!")
            log_widget.write_line(f"   Recipients: {len(self.recipients)}")
            log_widget.write_line(f"   Accounts: {len(self.sender.accounts)}")
            log_widget.write_line(f"   Workers: {self.sender.worker_count}")
            log_widget.write_line(
                f"   Attachments: {'✅ ENABLED' if self.sender.attachment_enabled else '❌ DISABLED'}"
            )
            log_widget.write_line(
                f"   Sender Names: {self.sender.sender_name_rotator.get_stats()['total_names']} available"
            )
            log_widget.write_line(
                f"   Rotation Strategy: {self.sender.sender_name_rotator.current_strategy}"
            )
            log_widget.write_line(f"   Company Brand: {self.sender.company_brand}")
            if self.sender.attachment_enabled:
                log_widget.write_line(
                    f"   Attachment Format: {self.sender.stats['current_format']}"
                )
        except:
            print("Starting email sending process...")

        asyncio.create_task(self.send_emails_parallel())

    def stop_sending(self):
        if self.sender:
            self.sender.is_running = False
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line("⏹️ EMAIL SENDING STOPPED!")
            except:
                print("Stopping email sending process...")

    async def send_emails_parallel(self):
        """Enhanced hybrid email sending with both single and parallel processing"""
        if not self.recipients:
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line("❌ No recipients found!")
            except:
                print("No recipients found!")
            return

        remaining_recipients = self.recipients[self.current_recipient_index :]
        total_recipients = len(remaining_recipients)

        batch_size = self.sender.get_optimal_batch_size(total_recipients)
        use_parallel = batch_size > 1

        if use_parallel:
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line(
                    f"🔄 Using parallel processing with {batch_size} workers"
                )
            except:
                print(f"Using parallel processing with {batch_size} workers")

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=batch_size
            ) as executor:
                while remaining_recipients and self.sender.is_running:
                    current_batch_size = min(len(remaining_recipients), 10 * batch_size)
                    current_batch = [
                        (self.current_recipient_index + i, recipient)
                        for i, recipient in enumerate(
                            remaining_recipients[:current_batch_size]
                        )
                    ]

                    futures = [
                        executor.submit(
                            self.sender.process_single_email, index, recipient
                        )
                        for index, recipient in current_batch
                    ]

                    for future in concurrent.futures.as_completed(futures):
                        try:
                            recipient, success, message = future.result()
                            self.log_email_result(recipient, success, message)
                            self.current_recipient_index += 1
                        except Exception as e:
                            print(f"Error processing email: {e}")

                    remaining_recipients = self.recipients[
                        self.current_recipient_index :
                    ]
                    await asyncio.sleep(0.5)
        else:
            try:
                log_widget = self.query_one("#activity-log", Log)
                log_widget.write_line("📧 Using sequential processing for small batch")
            except:
                print("Using sequential processing for small batch")

            for index, recipient in enumerate(remaining_recipients):
                if not self.sender.is_running:
                    break

                recipient_index = self.current_recipient_index + index
                result = self.sender.process_single_email(recipient_index, recipient)
                recipient, success, message = result
                self.log_email_result(recipient, success, message)
                self.current_recipient_index += 1

                await asyncio.sleep(1)

        self.sender.is_running = False
        try:
            log_widget = self.query_one("#activity-log", Log)
            log_widget.write_line("🏁 EMAIL SENDING COMPLETED!")
        except:
            print("Email sending completed!")

    def update_stats(self):
        if not self.sender:
            return

        try:
            stats = self.sender.get_stats()

            email_table = self.query_one("#email-stats-table", DataTable)
            email_table.clear()

            email_table.add_row("Total Recipients", str(len(self.recipients)))
            email_table.add_row(
                "Remaining", str(len(self.recipients) - self.current_recipient_index)
            )
            email_table.add_row("Successfully Sent", str(stats["total_sent"]))
            email_table.add_row("Failed", str(stats["total_failed"]))

            success_rate = 0
            if stats["total_sent"] + stats["total_failed"] > 0:
                success_rate = (
                    stats["total_sent"] / (stats["total_sent"] + stats["total_failed"])
                ) * 100
            email_table.add_row("Success Rate", f"{success_rate:.1f}%")
            email_table.add_row(
                "Status", "🟢 RUNNING" if stats["is_running"] else "🔴 STOPPED"
            )

            # Enhanced attachment status display with proper format
            attachment_status = (
                "✅ ENABLED" if stats["attachment_enabled"] else "❌ DISABLED"
            )
            email_table.add_row("Attachments", attachment_status)

            if stats["attachment_enabled"]:
                current_format_display = stats.get("current_format", "PDF")
                email_table.add_row("Current Format", current_format_display)
            else:
                email_table.add_row("Current Format", "N/A (Disabled)")

            # Enhanced branding display
            current_sender_display = stats.get("current_sender_name", "BusinessPal")
            company_brand_display = stats.get("company_brand", "BusinessPal Solutions")

            email_table.add_row("Current Sender", current_sender_display)
            email_table.add_row("Company Brand", company_brand_display)

            if self.recipients and self.current_recipient_index < len(self.recipients):
                remaining = len(self.recipients) - self.current_recipient_index
                estimated_time = self.sender.estimate_time_remaining(remaining)
                email_table.add_row("Estimated Time", estimated_time)
                email_table.add_row(
                    "Sending Rate", f"{stats['sending_rate']:.2f} emails/sec"
                )
                email_table.add_row("Workers", str(stats["worker_count"]))

            account_table = self.query_one("#account-stats-table", DataTable)
            account_table.clear()
            for account in stats["accounts"]:
                email_short = (
                    account["email"][:25] + "..."
                    if len(account["email"]) > 25
                    else account["email"]
                )
                status_icon = {
                    "idle": "⚪",
                    "active": "🟢",
                    "sending": "🟡",
                    "authenticating": "🟠",
                    "failed": "🔴",
                    "error": "❌",
                }.get(account.get("status", "idle"), "⚪")

                account_table.add_row(
                    email_short,
                    str(account["daily_sent"]),
                    f"{status_icon} {account.get('status', 'idle')}",
                    f"{account['daily_sent']}/{account['max_daily_limit']}",
                )

            template_table = self.query_one("#template-stats-table", DataTable)
            template_table.clear()
            template_table.add_row(
                "Email Templates",
                str(stats["email_templates"]),
                stats.get("current_template", "None"),
            )
            template_table.add_row(
                "Attachment Templates",
                str(stats["attachment_templates"]),
                stats.get("current_attachment_template", "None")
                if stats["attachment_enabled"]
                else "Disabled",
            )
            template_table.add_row(
                "Subject Templates", str(stats["subject_templates"]), "Rotating"
            )
            template_table.add_row(
                "Custom Fields",
                str(len(stats["custom_fields"])),
                ", ".join(stats["custom_fields"][:3]),
            )

            if self.recipients:
                progress = (self.current_recipient_index / len(self.recipients)) * 100
                progress_bar = self.query_one("#progress-bar", ProgressBar)
                progress_bar.update(progress=progress)

                progress_text = self.query_one("#progress-text", Label)
                progress_text.update(
                    f"📧 {self.current_recipient_index} / {len(self.recipients)} emails processed"
                )

        except Exception as e:
            print(f"Error updating stats: {e}")

    def update_sender_rotation_stats(self):
        """Update sender rotation statistics display"""
        try:
            sender_table = self.query_one("#sender-rotation-table", DataTable)
            sender_table.clear()

            sender_stats = self.sender.sender_name_rotator.get_stats()

            sender_table.add_row("Total Names", str(sender_stats["total_names"]))
            sender_table.add_row("Strategy", sender_stats["current_strategy"])
            sender_table.add_row("Current Index", str(sender_stats["current_index"]))
            sender_table.add_row(
                "Has Weights", "Yes" if sender_stats["has_weights"] else "No"
            )

            if sender_stats["sample_names"]:
                sample_display = ", ".join(sender_stats["sample_names"])
                if len(sample_display) > 40:
                    sample_display = sample_display[:37] + "..."
                sender_table.add_row("Sample Names", sample_display)

        except Exception as e:
            print(f"Error updating sender rotation stats: {e}")

    def update_file_stats(self):
        try:
            file_table = self.query_one("#file-stats-table", DataTable)
            file_table.clear()

            try:
                with open("gmail_accounts.txt", "r") as f:
                    accounts_count = len([line for line in f if ":" in line])
            except FileNotFoundError:
                accounts_count = 0

            try:
                with open("mails.txt", "r") as f:
                    recipients_count = len([line for line in f if "@" in line])
            except FileNotFoundError:
                recipients_count = 0

            file_table.add_row("📧 Gmail Accounts", str(accounts_count))
            file_table.add_row("📮 Recipients", str(recipients_count))
        except Exception as e:
            print(f"Error updating file stats: {e}")

    def update_template_stats(self):
        try:
            template_table = self.query_one("#template-stats-table", DataTable)
            template_table.clear()

            email_templates = len(glob.glob("email/*.html"))
            attachment_templates = len(glob.glob("attachment/*.html"))

            template_table.add_row(
                "📄 Email Templates", str(email_templates), "🔄 Rotating"
            )
            if self.sender and self.sender.attachment_enabled:
                template_table.add_row(
                    "📎 Attachment Templates", str(attachment_templates), "🔄 Rotating"
                )
            else:
                template_table.add_row(
                    "📎 Attachment Templates", str(attachment_templates), "❌ Disabled"
                )
        except Exception as e:
            print(f"Error updating template stats: {e}")

    def update_subject_rotation_stats(self):
        try:
            subject_table = self.query_one("#subject-rotation-table", DataTable)
            subject_table.clear()

            for i, subject in enumerate(self.sender.subject_rotator.subject_templates):
                status = (
                    "🔄 Active"
                    if i
                    == (self.sender.subject_rotator.subject_index - 1)
                    % len(self.sender.subject_rotator.subject_templates)
                    else "⏸️ Waiting"
                )
                subject_short = subject[:50] + "..." if len(subject) > 50 else subject
                subject_table.add_row(subject_short, status)
        except Exception as e:
            print(f"Error updating subject rotation stats: {e}")


def main():
    sender = EnhancedGmailBulkSender()

    # Load configuration
    config_loaded = sender.load_config("config.txt")
    if not config_loaded:
        print("⚠️ Warning: Could not load config.txt, using default values")
        print("📄 Creating enhanced sample config.txt with sender rotation...")

        # Create enhanced sample config with sender rotation and all new features
        sample_config = """[EMAIL]
subject1 = Invoice {invoice_number} - Order #{CUSTOM_POST_CODE} - {current_date}
subject2 = Payment Receipt {invoice_category} - {phone_number}
subject3 = Order Confirmation {RANDOM_ALPHA_3}-{invoice_number}
subject4 = Friday Meeting in Calendar – {name} – {date}
subject5 = {company_name} - Your Order {RANDOM_NUM_6} is Ready
subject6 = Hello {NAME}, Account {RANDOM_ALPHANUMERIC_8} Updated
subject7 = {sender_name} - Delivery {CUSTOM_DATE_+7_YYYY-MM-DD}
subject8 = Urgent: Action Required - {name}
subject9 = Welcome to {company_name} - Getting Started
subject10 = Thank You {name} - Order Complete
subject_strategy = sequential
company_brand = BusinessPal Solutions

[SENDER_ROTATION]
sender_names = BusinessPal,No Reply,Bake Bros,Guffy Shop,TechCorp Solutions,Global Enterprises,NextGen Systems,InnovateInc,FutureTech Labs,Prime Business
strategy = sequential

[ATTACHMENT]
attachment = true
format = PDF
rotate_formats = true
selected_formats = PDF,JPG,PNG

[LIMITS]
rotation_limit = 10
daily_limit = 300
chunk_size = 100

[RATE_LIMITING]
requests_per_second = 0.5
retry_delay = 8
max_retries = 5
batch_delay = 10

[PARALLEL]
workers = 2

[CUSTOM_FIELDS]
custom_post_code = 9899,9000,9999
custom_region = North,South,East,West
custom_priority = High,Medium,Low
"""

        with open("config.txt", "w") as f:
            f.write(sample_config)

        # Reload with sample config
        sender.load_config("config.txt")

    # Load accounts
    accounts_loaded = sender.load_gmail_accounts("gmail_accounts.txt")
    if not accounts_loaded:
        print("⚠️ Warning: No Gmail accounts found in gmail_accounts.txt")
        print("   Create gmail_accounts.txt with format: email:password (one per line)")
    else:
        print(f"✅ Loaded {len(accounts_loaded)} Gmail accounts")

    # Create required directories
    for directory in ["email", "attachment"]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"📁 Created directory: {directory}/")

    # Check for template files
    email_templates = len(glob.glob("email/*.html"))
    attachment_templates = len(glob.glob("attachment/*.html"))

    print(f"📄 Email templates found: {email_templates}")
    print(f"📎 Attachment templates found: {attachment_templates}")

    if email_templates == 0:
        print("⚠️ Warning: No email templates found in email/ directory")

    if sender.attachment_enabled and attachment_templates == 0:
        print(
            "⚠️ Warning: Attachments enabled but no templates found in attachment/ directory"
        )

    # Check dependencies
    if sender.attachment_enabled and not CONVERSION_AVAILABLE:
        print("❌ Warning: Attachment conversion dependencies missing!")
        print("   Install: pip install pillow imgkit pdfkit")
        print("   Install system dependencies: wkhtmltopdf, wkhtmltoimage")

    print(f"\n🚀 Starting Enhanced Gmail Bulk Sender Dashboard...")
    sender_stats = sender.sender_name_rotator.get_stats()
    print(f"👤 Sender Names: {sender_stats['total_names']} available")
    print(f"🔄 Rotation Strategy: {sender_stats['current_strategy']}")
    print(f"🏢 Company Brand: {sender.company_brand}")
    print(
        f"📎 Attachments: {'✅ ENABLED' if sender.attachment_enabled else '❌ DISABLED'}"
    )
    if sender.attachment_enabled:
        print(f"📁 Current Format: {sender.stats['current_format']}")

    app = GmailDashboard(sender)
    app.run()


if __name__ == "__main__":
    main()

import logging

logger = logging.getLogger(__name__)

def normalize_email(email):
    return email.lower().strip() if email else ""

def validate_email(email):
    email = normalize_email(email)
    if not email:
        return False, "Email is empty"
    if email.count("@") != 1:
        return False, "Email must have exactly one @"
    local, domain = email.split("@")
    if not local or not domain:
        return False, "Email missing local or domain"
    if " " in email:
        return False, "Email cannot have spaces"
    return True, None

def sync_contractors_from_monday(engine=None):
    return {"checked": 0, "added": 0, "updated": 0, "unchanged": 0, "needs_review": 0, "failed": 0, "details": []}

def cli_main():
    print("Contractor sync ready")

if __name__ == "__main__":
    cli_main()

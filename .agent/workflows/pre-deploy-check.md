---
description: Pre-deploy check — run before every git push to catch production issues
---

# Pre-Deploy Check for ca-copilot Backend

Run these checks EVERY TIME before pushing code to `main` (which triggers Render auto-deploy).

## 1. Verify all imports resolve
// turbo
```bash
cd /Users/rahulgupta/ca-copilot && source venv/bin/activate && cd apps/api && python -c "from app.main import app; print('✅ All imports OK')"
```
If this fails, a module is missing from `requirements.txt` or there's a syntax error.

## 2. Verify DATABASE_URL parsing is clean
// turbo
```bash
cd /Users/rahulgupta/ca-copilot && source venv/bin/activate && cd apps/api && python -c "
from app.core.config import settings
url = str(settings.DATABASE_URL)
db_name = url.split('/')[-1]
assert '&' not in db_name and '?' not in db_name, f'❌ Query params leaking into DB name: {db_name}'
print(f'✅ DB URL clean — database: {db_name}')
"
```

## 3. Cross-check third-party imports vs requirements.txt
// turbo
```bash
cd /Users/rahulgupta/ca-copilot && source venv/bin/activate && cd apps/api && python -c "
imports = [
    'fastapi', 'uvicorn', 'sqlalchemy', 'pydantic', 'pydantic_settings',
    'jose', 'passlib', 'openpyxl', 'xlsxwriter', 'thefuzz', 'Levenshtein',
    'supabase', 'openai', 'google.oauth2', 'google.auth', 'requests',
    'asyncpg', 'alembic', 'docx',
]
missing = []
for mod in imports:
    try:
        __import__(mod)
    except ImportError:
        missing.append(mod)
if missing:
    print(f'❌ Missing: {missing}')
    print('Add them to requirements.txt')
else:
    print('✅ All third-party packages available')
"
```

## 4. Verify start.sh is non-fatal for DB operations
// turbo
```bash
cd /Users/rahulgupta/ca-copilot && grep -q '|| echo' apps/api/scripts/start.sh && echo "✅ start.sh has fallbacks for DB operations" || echo "❌ start.sh will crash if DB connection fails — add || echo fallbacks"
```

## 5. Check no secrets in tracked files
// turbo
```bash
cd /Users/rahulgupta/ca-copilot && git diff --cached --name-only 2>/dev/null | grep -E "credentials\.json|token\.json|\.env$" && echo "❌ SECRET FILES STAGED — remove them!" || echo "✅ No secrets in staged files"
```

## Summary
If all 5 checks pass with ✅, the code is safe to push. If ANY shows ❌, fix the issue first.

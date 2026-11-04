# הגדרת גשר NotebookLM

## 1. התחברות לגוגל (פעם אחת, במחשב שלך)

מומלץ חשבון גוגל ייעודי לתמלול, לא החשבון האישי.

```
pip install "notebooklm-py[browser]"
notebooklm login
notebooklm auth check --test --json     # צריך להחזיר "status": "ok"
```

הקובץ נשמר ב-`~/.notebooklm/storage_state.json`. התוכן שלו (JSON אחד) הוא הערך של `NOTEBOOKLM_AUTH_JSON`.

הקובץ הזה הוא אישור גישה לחשבון: לא לשלוח אותו בצ'אט ולא לשמור אותו ב-GitHub.
כשהעוגיות פגות, יש לחדש אותו. לריצה ללא השגחה אפשר להשתמש ב-`notebooklm login --master-token` (ראו README של הספרייה).

## 2. משתני סביבה בשרת הגשר

| משתנה | ערך |
|---|---|
| `BRIDGE_TOKEN` | מחרוזת אקראית ארוכה (למשל `openssl rand -hex 32`) |
| `ALLOWED_MEDIA_ORIGIN` | מקורות מותרים לקובצי השמע, מופרדים בפסיקים, בלי סלאש בסוף. לאתר התמלולים ב-Vercel: `https://<project>.supabase.co/storage/v1/object/sign/uploads` |
| `NOTEBOOKLM_AUTH_JSON` | תוכן storage_state.json |
| `FOLDER_PREFIX` | (לא חובה) שם המחברות, ברירת מחדל `שיעורים` ← "שיעורים 001", "שיעורים 002"… |
| `SOURCE_LIMIT` | (לא חובה) כמה מקורות במחברת לפני שנפתחת חדשה, ברירת מחדל 50 (בחשבון Pro אפשר 300) |

## 3. העלאה

הפרויקט כולל `Dockerfile`. יש להריץ מופע אחד בלבד (המשימות בזיכרון).
בדיקה: `GET {כתובת הגשר}/health` צריך להחזיר `{"ok": true}`.

## 4. חיבור לאפליקציה

בהגדרות הסודות של הפרויקט בלובובל:

- `NOTEBOOKLM_BRIDGE_URL` = כתובת הגשר
- `NOTEBOOKLM_BRIDGE_TOKEN` = אותו ערך של `BRIDGE_TOKEN`

ואז באפליקציה המפורסמת: בפאנל התמלול של שיעור, "מנוע תמלול" ← NotebookLM.

## 5. חיבור לאתר התמלולים (shiur-transcripts ב-Vercel)

ב-Vercel ← Project Settings ← Environment Variables:

- `NOTEBOOKLM_BRIDGE_URL` = כתובת הגשר
- `NOTEBOOKLM_BRIDGE_TOKEN` = אותו ערך של `BRIDGE_TOKEN`

ובגשר יש להוסיף ל-`ALLOWED_MEDIA_ORIGIN` את כתובת האחסון של Supabase (ראו טבלה למעלה).
אחרי פריסה מחדש, בהעלאת קובץ שמע מופיעה בחירת מנוע: NotebookLM.

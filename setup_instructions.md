Quick Setup Instructions
Step 1: Create Repository Structure
bash
# Create main folder
mkdir companies-house-screener
cd companies-house-screener

# Create subfolders
mkdir worker
mkdir ui
mkdir ui/.streamlit
mkdir docs

# Initialize git
git init
Step 2: Copy Files
Worker Files

Copy these files into worker/ folder:

worker.py → worker/worker.py

worker_requirements.txt → worker/requirements.txt

UI Files

Copy these files into ui/ folder:

ui_app.py → ui/app.py

ui_requirements.txt → ui/requirements.txt

secrets_template.toml → ui/.streamlit/secrets_template.toml

Root Files

Copy these to root folder:

README.md → README.md

gitignore.txt → .gitignore

Step 3: Configure Secrets
Copy the secrets template:

bash
cp ui/.streamlit/secrets_template.toml ui/.streamlit/secrets.toml
Edit ui/.streamlit/secrets.toml:

Add your Supabase DATABASE_URL

Add your RESTRICTED_SIC_CODES

DO NOT COMMIT secrets.toml - it's in .gitignore

Step 4: Commit to Git
bash
git add .
git commit -m "Initial commit: Companies House screener"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/companies-house-screener.git
git push -u origin main
Step 5: Deploy
Deploy Worker to Render

Go to https://dashboard.render.com

New → Web Service

Connect GitHub → Select companies-house-screener

Root Directory: worker

Build Command: pip install -r requirements.txt

Start Command: python worker.py

Instance Type: Starter ($7/month)

Add environment variables:

DATABASE_URL

COMPANIES_HOUSE_STREAMING_API_KEY

Deploy UI to Streamlit

Go to https://streamlit.io/cloud

New app

Select repo: companies-house-screener

File path: ui/app.py

Deploy

Add secrets in app settings

File Checklist
worker/worker.py

worker/requirements.txt

ui/app.py

ui/requirements.txt

ui/.streamlit/secrets.toml (DO NOT COMMIT)

.gitignore

README.md

Next Steps
Set up Supabase database (run schema SQL)

Get Companies House API key

Deploy worker to Render

Deploy UI to Streamlit

Test that companies are appearing

See README.md for full details!

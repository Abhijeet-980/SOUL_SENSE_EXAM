# 📊 GitHub Project Board Setup for Contributors

## Overview

This guide helps contributors coordinate using GitHub's built-in project boards WITHOUT needing admin permissions.

## How to Use (No Permissions Needed):

### Option A: Personal Project Board (Anyone can create)

1. Go to your GitHub profile → Projects → New Project
2. Name: "Soul Sense - My Contributions"
3. Template: Basic Kanban
4. Make it **Public**
5. Add issues by pasting their URLs

### Option B: Fork-Based Coordination Board

1. Fork the repository
2. In YOUR fork: Projects → New Project
3. Name: "Community Coordination"
4. Share link: `https://github.com/YOUR-USERNAME/soul-sense-exam/projects/1`

## Recommended Board Structure:

📋 BACKLOG
├── Good First Issues
├── Help Wanted
└── Future Features

🎯 IN PROGRESS
├── Backend (Python/FastAPI)
├── Frontend (Next.js/React)
└── Mobile/Other

🔍 REVIEW NEEDED
├── Code Review
├── Design Review
└── Documentation Review

✅ COMPLETED
├── This Week
├── This Month
└── All Time

## Automation (Using GitHub Actions):

See `.github/workflows/project-automation.yml` for automatic issue tracking.

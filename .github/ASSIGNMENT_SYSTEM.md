# 🎯 Contributor Self-Assignment System

## Overview

This repository uses an automated self-assignment system. Contributors can claim issues, track their workload, and coordinate with others—all through simple commands.

---

## 📌 Available Commands

| Command              | Description                         |
| -------------------- | ----------------------------------- |
| `/assign` or `/take` | Claim an issue for yourself         |
| `/unassign`          | Release an issue you're assigned to |
| `/status`            | Check your current assignments      |

---

## 🔄 How It Works

### Claiming an Issue

1. Find an issue you want to work on
2. Comment `/assign` on that issue
3. The bot will:
   - ✅ Assign you to the issue
   - 🏷️ Add `status: in-progress` label
   - 💬 Post a confirmation with next steps

### If the Issue is Already Taken

- The bot will tell you who's assigned
- It will suggest available alternatives
- You can pick a different issue

### Workload Management

- **3+ issues**: You'll get a friendly reminder
- **5+ issues**: Assignment blocked—complete some work first!
- This ensures fair distribution among all contributors

---

## 🏷️ Label System

| Label                    | Meaning                           |
| ------------------------ | --------------------------------- |
| `status: available`      | Ready for someone to claim        |
| `status: in-progress`    | Someone is actively working on it |
| `status: needs-review`   | PR submitted, awaiting review     |
| `status: blocked`        | Stuck, needs help                 |
| `good first issue`       | Great for beginners               |
| `first-time-contributor` | Author's first PR                 |

---

## 📊 Dashboard

A live dashboard is automatically maintained at a pinned issue titled **"📊 Community Contribution Dashboard"**.

It shows:

- 📈 Overall stats (open, closed, in-progress)
- 🌊 Wave progress (if using wave labels)
- 🌱 Good first issues for beginners
- 🚧 Issues needing help
- 👥 Top contributors

---

## 🎉 First-Time Contributors

New to this project? You'll receive:

- A personalized welcome message
- Links to contributing guides
- A `first-time-contributor` label on your first PR

---

## 💡 Tips

1. **Check `/status` regularly** to see what you're working on
2. **Use `/unassign`** if you can't complete something—it's okay!
3. **Look for `good first issue`** labels if you're new
4. **Don't hoard issues**—the system limits you to prevent burnout

---

## 🤖 Automation

All commands are handled automatically by GitHub Actions. No admin intervention needed!

The system runs 24/7 and updates the dashboard every 6 hours.

# Contributing Guide  

Welcome to the **NYC Train Mobility** project!  
This guide explains how we collaborate, commit code, and report issues. All templates are included here for convenience.  

---

## Project Workflow  

We use **GitHub flow** + **Agile Scrum practices**:  
1. Work in **feature branches**  
2. Submit changes via **Pull Requests (PRs)**  
3. Keep `main` branch stable  
4. Track tasks on our [Scrum Board](https://alustudent-team1.atlassian.net/jira/software/projects/MSPE/boards/34?atlOrigin=eyJpIjoiYjg2ZjViOGNhM2FhNDUzNmFhZDg1MzA5OTdlOGU3ZmMiLCJwIjoiaiJ9) 

---

## Branching Strategy  

- `main` → Stable branch (protected)  
- `dev` → Optional integration branch  
- `feature/<name>` → New features  
- `bugfix/<name>` → Bug fixes  
- `hotfix/<name>` → Urgent production fixes  

Example:  
```
git checkout -b feature/etl-cleaning
```

---

## Commit Guidelines

We follow conventional commits:  
- `feat:` – New feature  
- `fix:` – Bug fix  
- `docs:` – Documentation only  
- `test:` – Adding/updating tests  
- `chore:` – Maintenance  

Example:  
```
feat: add XML parser for MoMo SMS input
fix: correct date normalization edge cases
docs: update README with setup instructions
```

---

## Pull Request Process

- Sync branch with main.  
- Run tests locally:  
  ```
  pytest tests/
  ```  
- Open PR with description + related issue.  
- Request at least one review.  
- Merge after approval.

---

## Testing

- All new features must include unit tests.  
- Fixes should include regression tests.  
- Run tests:  
  ```
  pytest tests/
  ```

---

## Code Style

- Python → [PEP8](https://peps.python.org/pep-0008/) (black, flake8)  
- JavaScript → ESLint defaults  
- Naming → descriptive, consistent

---

## Templates

Below are the templates for Bug Reports, Feature Requests, and Pull Requests.  
Copy-paste into issues/PRs when contributing.

---

### Bug Report Template

#### Bug Description  
A clear and concise description of the bug.

#### Steps to Reproduce  
1. Go to '...'  
2. Run '...'  
3. See error

#### Expected Behavior  
What should have happened?

#### Screenshots / Logs  
Attach if available.

#### Environment  
- OS: [e.g. Ubuntu 22.04]  
- Python: [e.g. 3.11]  
- Browser: [e.g. Chrome 115]

#### Additional Context  
Any other details?

---

### Feature Request Template

#### Feature Description  
Describe the feature you'd like.

#### Why is this needed?  
Explain the problem this solves.

#### Proposed Solution  
Outline the approach.

#### Alternatives Considered  
Any alternatives?

#### Additional Context  
Mockups, diagrams, or references.

---

### Pull Request Template

#### Description  
What does this PR do?

#### Related Issue  
Closes #ISSUE_NUMBER

#### Type of Change  
- [ ] Feature  
- [ ] Bug Fix  
- [ ] Documentation  
- [ ] Tests  
- [ ] Chore  

#### How Has This Been Tested?  
Explain testing steps.

#### Screenshots (if applicable)  
Add images/logs.

#### Checklist  
- [ ] Code follows repo style  
- [ ] Self-review done  
- [ ] Tests added/updated  
- [ ] Docs updated  
- [ ] No secrets committed

---

**Thanks for contributing!**  
By following this guide, we'll keep the project organized, collaborative, and professional.

---

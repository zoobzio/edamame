# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| latest  | :white_check_mark: | Active development |
| < latest | :x: | Security fixes only for critical issues |

## Reporting a Vulnerability

We take the security of edamame seriously. If you have discovered a security vulnerability in this project, please report it responsibly.

### How to Report

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security tab](https://github.com/zoobzio/edamame/security) of this repository
   - Click "Report a vulnerability"
   - Fill out the form with details about the vulnerability

2. **Email**
   - Send details to the repository maintainer through GitHub profile contact information
   - Use PGP encryption if possible for sensitive details

### What to Include

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- **Type of issue** (e.g., SQL injection, race condition, information disclosure, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration required** to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit the issue
- **Your name and affiliation** (optional)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: Within 7 days, we will provide an initial assessment of the report
- **Resolution Timeline**: We aim to resolve critical issues within 30 days
- **Disclosure**: We will coordinate with you on the disclosure timeline

### Preferred Languages

We prefer all communications to be in English.

## Security Best Practices

When using edamame in your applications, we recommend:

1. **Keep Dependencies Updated**
   ```bash
   go get -u github.com/zoobzio/edamame
   ```

2. **Database Security**
   - Use least-privilege database accounts
   - Never expose factory specs publicly without authentication
   - Validate all user input before passing to capabilities
   - Use transactions for multi-step operations

3. **LLM Integration Security**
   - Always validate LLM-generated capability names and params
   - Rate limit LLM-driven database operations
   - Audit log all LLM-executed queries
   - Never expose raw SQL generation to external systems

4. **Capability Management**
   - Only expose capabilities that are safe for your use case
   - Use explicit params with validation
   - Avoid overly permissive WHERE clauses
   - Review auto-derived params before deployment

5. **Error Handling**
   - Implement proper error handling
   - Don't expose internal errors to users
   - Log errors appropriately

## Security Features

edamame includes several built-in security features:

- **Parameterized Queries**: All queries use parameterized SQL via sqlx, preventing SQL injection
- **Type Safety**: Generic types prevent type confusion
- **Spec Validation**: Capability specs are validated before execution
- **No Raw SQL**: Users define specs, not raw SQL strings
- **Introspection Only**: Spec export is read-only, doesn't expose credentials

## Automated Security Scanning

This project uses:

- **CodeQL**: GitHub's semantic code analysis for security vulnerabilities
- **gosec**: Go security checker for common vulnerabilities
- **golangci-lint**: Static analysis including security linters (sqlclosecheck, noctx, bodyclose)
- **Codecov**: Coverage tracking to ensure security-critical code is tested

## Vulnerability Disclosure Policy

- Security vulnerabilities will be disclosed via GitHub Security Advisories
- We follow a 90-day disclosure timeline for non-critical issues
- Critical vulnerabilities may be disclosed sooner after patches are available
- We will credit reporters who follow responsible disclosure practices

## Credits

We thank the following individuals for responsibly disclosing security issues:

_This list is currently empty. Be the first to help improve our security!_

---

**Last Updated**: 2025-12-17

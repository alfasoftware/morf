# Security Policy for Morf

## Reporting a Vulnerability

The Morf team takes security vulnerabilities seriously. We appreciate your efforts to responsibly disclose your findings and will make every effort to address security issues quickly.

To report a security vulnerability, please follow these steps:

1. **DO NOT** create a public GitHub issue for the vulnerability.
2. Email your findings to [security@alfasoftware.com](mailto:security@alfasoftware.com). If possible, encrypt your message using our PGP key (available upon request).
3. Include the following information in your report:
   - Description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact of the vulnerability
   - Any potential mitigations you've identified

## What to Expect

When you submit a vulnerability report, you can expect the following:

- Acknowledgment of your report within 48 hours
- An initial assessment of the report within 7 days
- Regular updates on our progress addressing the vulnerability
- Credit for the discovery (if desired) once the issue is resolved

## Disclosure Policy

- We follow a coordinated disclosure process
- We will work with you to understand and address the issue before any public disclosure
- We aim to release fixes for confirmed vulnerabilities within 90 days
- Once a fix is available, we will publish information about the vulnerability

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |
| < latest | :x:                |

Generally, only the most recent release of Morf receives security updates. We encourage all users to keep their installations up to date.

## Security Best Practices

When using Morf in your projects:

1. Always use the latest version
2. Follow the principle of least privilege when configuring database access
3. Regularly review and audit your database schemas and migrations
4. Keep your development environment secure and up to date

## Security Updates

Security updates will be released as part of our regular release process and will be noted in the release notes. Critical security fixes may be released out-of-band as needed.

## Code Security

We strive to maintain secure coding practices:

- All code changes undergo peer review
- We use automated security scanning tools
- We follow secure development best practices
- We regularly update dependencies to address known vulnerabilities

---

This security policy is subject to change. Last updated: [Current Date].

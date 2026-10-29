# Claude Code configuration for SignalOps

Copy CLAUDE.md to the repository root and .claude/settings.json to the repository's .claude directory. Keep development_roadmap.md and SignalOps_Implementation_Plan.md at the root as referenced by the instructions. Extract the archive with hidden directories preserved.

If you already have either configuration file, merge the contents instead of overwriting your custom instructions, hooks, or permissions. Commit shared project configuration if desired; keep personal overrides in .claude/settings.local.json and exclude that local file from git.

Settings use acceptEdits to approve routine file edits, plus narrowly named lint, test, build, and read-only inspection commands. Dependency installation, container startup/exec, migrations, commits, push, deployment, and arbitrary shell/Python commands are not broadly preapproved. An unmatched command follows Claude Code's normal permission flow. Tests and build scripts execute project code; this is not an OS sandbox or a complete secret-access policy.

No model is pinned, no automatic hooks are installed, and no provider keys or evaluation budget are configured. This avoids paid evaluations or commands that do not exist in the unbootstrapped repository. CLAUDE.md describes proposed uv/pnpm conventions; adapt them when implementation establishes actual commands.

After copying, start Claude Code from the repository root. Use /permissions to inspect loaded rules and /memory to inspect project instructions. Existing local, user, or managed settings may affect the effective configuration. Commands with a different runner or flags before the named subcommand can require separate approval.

Suggested first prompt:

Read CLAUDE.md and development_roadmap.md. Inspect the repository and implement P01-T01, then run the applicable acceptance checks and update the roadmap. Do not commit yet.

Official references checked 29 October 2026:
- https://code.claude.com/docs/en/settings
- https://code.claude.com/docs/en/settings-reference
- https://code.claude.com/docs/en/permissions
- https://support.claude.com/en/articles/14553240-give-claude-context-claude-md-and-better-prompts

The JSON was parsed and its settings checked against documentation; it has not been loaded into your local Claude Code installation.

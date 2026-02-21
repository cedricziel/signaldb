---
name: review-comments
description: Interact with GitHub PR review comments and threads - fetch, analyze, reply, and resolve.
user-invocable: true
---

# Review Comments Skill

Comprehensive toolkit for interacting with GitHub Pull Request review comments and threads using `gh` CLI and GraphQL API.

## Quick Commands

```bash
# Invoke this skill
/review-comments [PR_NUMBER]

# Fetch all review comments for current PR
gh pr view --json reviews,comments

# Fetch CodeRabbit review from PR
gh pr view PR_NUMBER --json body,comments --jq '.body, .comments[].body' | grep -A 1000 "coderabbitai"
```

## Usage Patterns

### 1. Fetch Review Threads (Including CodeRabbit)

Fetch all review threads with unresolved status:

```bash
OWNER="owner-name"
REPO="repo-name"
PR_NUMBER=123

gh api graphql -f query="
{
  repository(owner: \"$OWNER\", name: \"$REPO\") {
    pullRequest(number: $PR_NUMBER) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          isOutdated
          comments(first: 100) {
            nodes {
              id
              body
              path
              line
              author { login }
              createdAt
            }
          }
        }
      }
    }
  }
}"
```

### 2. List Only Unresolved Threads

Get unresolved threads with file paths and line numbers:

```bash
gh api graphql -f query="
{
  repository(owner: \"$OWNER\", name: \"$REPO\") {
    pullRequest(number: $PR_NUMBER) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 1) {
            nodes {
              path
              line
              body
              author { login }
            }
          }
        }
      }
    }
  }
}" | jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)'
```

### 3. Reply to a Review Thread

Add a reply to an existing review thread:

```bash
THREAD_ID="PRRT_kwDO..."  # From thread listing
REPLY_TEXT="Fixed in commit abc123"

gh api graphql -f query="
mutation {
  addPullRequestReviewThreadReply(input: {
    pullRequestReviewThreadId: \"$THREAD_ID\"
    body: \"$REPLY_TEXT\"
  }) {
    comment {
      id
      body
    }
  }
}"
```

### 4. Resolve a Review Thread

Mark a thread as resolved:

```bash
THREAD_ID="PRRT_kwDO..."

gh api graphql -f query="
mutation {
  resolveReviewThread(input: {
    threadId: \"$THREAD_ID\"
  }) {
    thread {
      id
      isResolved
    }
  }
}"
```

### 5. Unresolve a Review Thread

Reopen a resolved thread:

```bash
gh api graphql -f query="
mutation {
  unresolveReviewThread(input: {
    threadId: \"$THREAD_ID\"
  }) {
    thread {
      id
      isResolved
    }
  }
}"
```

## Working with CodeRabbit Reviews

CodeRabbit posts reviews as PR comments. Here's how to extract and parse them:

### Extract CodeRabbit Summary

```bash
# Get CodeRabbit's review summary
gh pr view $PR_NUMBER --json comments --jq '
  .comments[] |
  select(.author.login == "coderabbitai") |
  .body
'
```

### Parse CodeRabbit File Comments

CodeRabbit comments on specific files with markdown formatting:

```bash
# Extract file-specific feedback
gh pr view $PR_NUMBER --json comments --jq '
  .comments[] |
  select(.author.login == "coderabbitai" and .path != null) |
  {
    file: .path,
    line: .line,
    body: .body,
    id: .id
  }
'
```

### Count Actionable vs Nitpick Comments

CodeRabbit often categorizes comments:

```bash
# Count comment types
gh pr view $PR_NUMBER --json comments --jq '
  [.comments[] |
   select(.author.login == "coderabbitai")] |
  {
    total: length,
    actionable: [.[] | select(.body | contains("actionable"))] | length,
    nitpicks: [.[] | select(.body | contains("nitpick"))] | length
  }
'
```

## Automation Workflow

### Complete Review Response Workflow

1. **Fetch unresolved threads:**
   ```bash
   gh api graphql -f query=... | jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)'
   ```

2. **Address each comment** (make code changes)

3. **Reply to thread:**
   ```bash
   gh api graphql -f query='mutation { addPullRequestReviewThreadReply(...) }'
   ```

4. **Resolve thread:**
   ```bash
   gh api graphql -f query='mutation { resolveReviewThread(...) }'
   ```

5. **Commit and push:**
   ```bash
   git add .
   git commit -m "fix: address review comment in file.rs:123"
   git push
   ```

## Helper Functions

Add these to your shell or skill scripts:

```bash
# Get repository info from current directory
get_repo_info() {
  gh repo view --json owner,name --jq '{owner: .owner.login, name: .name}'
}

# Get current PR number
get_current_pr() {
  gh pr view --json number --jq '.number'
}

# List all unresolved threads for current PR
list_unresolved() {
  local pr_num=$(get_current_pr)
  local repo_info=$(get_repo_info)
  local owner=$(echo $repo_info | jq -r '.owner')
  local repo=$(echo $repo_info | jq -r '.name')

  gh api graphql -f query="
  {
    repository(owner: \"$owner\", name: \"$repo\") {
      pullRequest(number: $pr_num) {
        reviewThreads(first: 100) {
          nodes {
            id
            isResolved
            comments(first: 1) {
              nodes { path line body author { login } }
            }
          }
        }
      }
    }
  }" | jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)'
}

# Resolve thread with reply
resolve_with_reply() {
  local thread_id="$1"
  local message="$2"

  # Add reply
  gh api graphql -f query="
  mutation {
    addPullRequestReviewThreadReply(input: {
      pullRequestReviewThreadId: \"$thread_id\"
      body: \"$message\"
    }) {
      comment { id }
    }
  }"

  # Resolve thread
  gh api graphql -f query="
  mutation {
    resolveReviewThread(input: {
      threadId: \"$thread_id\"
    }) {
      thread { id isResolved }
    }
  }"
}
```

## Best Practices

1. **Always fetch thread ID before replying** - Thread IDs are stable across PR updates
2. **Include commit references in replies** - Help reviewers verify fixes
3. **Resolve threads after pushing fixes** - Shows progress and keeps reviews organized
4. **Use batch operations for multiple threads** - More efficient than one-by-one
5. **Test GraphQL queries first** - Use `--jq '.'` to see full response structure
6. **Check for CodeRabbit updates** - Bot may add new comments after pushes

## Error Handling

Common issues and solutions:

- **"Resource not accessible by integration"**: Check GitHub token permissions
- **"Thread not found"**: Thread may have been deleted or PR closed
- **"Invalid thread ID format"**: Ensure ID starts with `PRRT_kwDO`
- **GraphQL rate limiting**: Use `--jq '.errors'` to check for rate limit errors

## Integration with CI/CD

Example GitHub Actions workflow step:

```yaml
- name: Auto-resolve fixed review threads
  env:
    GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: |
    # Get PR number
    PR_NUM=$(gh pr view --json number -q .number)

    # List unresolved threads
    THREADS=$(gh api graphql -f query='...' | jq -r '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false) | .id')

    # Check if fixes exist in recent commits
    for thread_id in $THREADS; do
      # Logic to determine if thread is fixed
      # Then resolve it
      gh api graphql -f query="mutation { resolveReviewThread(input: {threadId: \"$thread_id\"}) { thread { id } } }"
    done
```

## References

- GitHub GraphQL API: https://docs.github.com/en/graphql/reference/mutations#resolvereviewthread
- gh CLI issue #12419: https://github.com/cli/cli/issues/12419
- CodeRabbit Documentation: https://docs.coderabbit.ai/

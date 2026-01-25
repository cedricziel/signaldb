# Task Completion Checklist

Before marking a task as complete:

1. **Build verification**
   ```bash
   cargo build
   ```

2. **Format code**
   ```bash
   cargo fmt
   ```

3. **Run clippy**
   ```bash
   cargo clippy --workspace --all-targets --all-features
   ```

4. **Check unused dependencies**
   ```bash
   cargo machete --with-metadata
   ```

5. **Run tests** (if applicable)
   ```bash
   cargo test
   ```

6. **For Grafana plugin changes**
   ```bash
   cd src/grafana-plugin && npm run build:backend
   ```
